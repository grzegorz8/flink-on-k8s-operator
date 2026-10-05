# FlinkCluster autoscaler

Autoscaling is crucial for streaming jobs with changing workloads. Matching resources to demand
helps reduce costs while maintaining enough capacity to process incoming data. Flink itself does not
autoscale jobs, so a separate autoscaler component is needed to decide when and how to adjust
resources.

The FlinkCluster operator does not provide an autoscaler implementation. It supports lifecycle
management of your own autoscaler, running as a separate Kubernetes Deployment. You supply the
image, Pod template, configuration, and required permissions; the operator manages the Deployment
and its configuration ConfigMap. This supports different autoscaling implementations, including
custom components based on Apache's `flink-autoscaler-standalone`.

## Responsibilities

| Component             | Responsibility                                                                                                        |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------|
| FlinkCluster operator | Create, update, recreate, and delete the autoscaler Deployment and configuration ConfigMap; report Deployment status. |
| Autoscaler image      | Collect metrics, decide when and how to scale, and apply scaling changes using its configured APIs.                   |
| Cluster owner         | Supply the image, target settings, credentials, service account, permissions, and scaling policy.                     |

## Configuration

The presence of `spec.autoscaler` enables the feature. There is no separate `enabled` flag. It is
supported only for Application jobs with explicit `execution.runtime-mode: STREAMING`. A TaskManager
HorizontalPodAutoscaler cannot be configured at the same time.

The following fragment shows the configuration structure; image and property values must be adapted
to the chosen implementation:

```yaml
spec:
  job:
    mode: Application
  flinkProperties:
    execution.runtime-mode: STREAMING
  autoscaler:
    podTemplate:
      spec:
        serviceAccountName: flink-autoscaler
        containers:
          - name: autoscaler
            image: REPLACE_WITH_YOUR_AUTOSCALER_IMAGE
    autoscalerProperties:
      example.setting: "replace-with-image-specific-value"
```

`autoscalerProperties` is a map of string keys and values. The operator writes it as YAML to
`data["config.yaml"]` in a dedicated ConfigMap. The file is mounted read-only at
`/opt/flink/conf/config.yaml` in every regular autoscaler container, using the managed
`autoscaler-config` volume and a `subPath` mount.

The Pod template supplies commands, environment variables, resources, probes, Secret references, and
other settings required by the image. Users create the service account and RBAC bindings. The
operator defaults restart policy to`Always` and container image pull policy to `IfNotPresent`,
preserving explicit values.

The operator checks eligibility, required containers and images, and conflicts with its managed
configuration volume and mount. Do not define the`autoscaler-config` volume or the
`/opt/flink/conf/config.yaml` mount in the Pod template. Kubernetes validates the generated
Deployment and supplies other Pod defaults.

## Service account and permissions

The scaler image runs with the service account named by`podTemplate.spec.serviceAccountName`. Create
and bind that account yourself. Grant only the API access the image needs. For example, a scaler
that reads a single named FlinkCluster and applies replica changes through its Kubernetes`/scale`
subresource can use a namespace Role with `get` on `flinkclusters` and `get`, `update`, and `patch`
on `flinkclusters/scale`, restricted to that resource name.

## Configuration updates and Flink job revisions

A checksum covers the entire `spec.autoscaler`, including the Pod template and properties. It is
recorded on both managed resources and the Deployment's Pod template. Changing the properties
updates the ConfigMap and triggers an autoscaler rollout so new Pods read the new configuration
file.

`spec.autoscaler` is excluded from Flink `ControllerRevision` data. Adding, editing, or removing
this section therefore does not restart the Flink job or require a savepoint for an autoscaler-only
update. Scaling actions that change the Flink job specification follow the normal Flink update
rules.

## Status and reconciliation

Autoscaler readiness does not affect Flink readiness. Autoscaler-only status changes allow Flink
reconciliation to continue. Errors observing or reconciling autoscaler resources fail the current
iteration and use normal controller retries.

## Example

Review and customize
the [autoscaler sample](../config/samples/flinkoperator_v1beta1_flinkcluster_autoscaler.yaml). Its
image is intentionally a placeholder: replace it with an image you supply and configure the
properties according to that image's contract before applying the sample.