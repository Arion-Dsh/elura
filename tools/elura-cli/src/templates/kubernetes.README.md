# Kubernetes deployment

The base uses DNS discovery with the headless `world` Service and mounts both runtime JSON files
from `elura-runtime-config`.

Before applying it, copy `secret.example.yaml` to the ignored `secret.yaml`, replace every
placeholder, and add the base plus `secret.yaml` to a private overlay. Then set the application
image and deploy that overlay:

```bash
kubectl apply -k path/to/private-overlay
```

Keep the generated Gateway at one replica while it uses the in-memory ticket replay store. Inject
a shared replay store in the application before scaling it horizontally.
