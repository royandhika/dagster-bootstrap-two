# dagster-bootstrap-two

Dagster with my custom needs, the specification:  
*dagster, dbt, sling, sqlserver, mysql*  
Also, made a docker compose script for deployment-ready that contains:
- database container (using mysql),
- webserver container,
- daemon container,
- grpc(code location) container

## Getting started

```bash
docker compose up -d --build
```

You can add as many grpc project as you want, just make sure to include it in `workspace.yaml`.

Each grpc container can have different library version or even different dependencies.

### Adding new Python dependencies

You can specify new Python dependencies in `requirements.txt`.

### Shared resources

Resources are made global in `shared/resources` and `shared/utils`, so multiple grpc container can use them without repeating the code itself.

### Unit testing

TBD

### Schedules and sensors

If you want to enable Dagster [Schedules](https://docs.dagster.io/guides/automate/schedules/) or [Sensors](https://docs.dagster.io/guides/automate/sensors/) for your jobs, the [Dagster Daemon](https://docs.dagster.io/guides/deploy/execution/dagster-daemon) process must be running. This is done automatically when you run `dagster dev`.  
TBD