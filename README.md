# Project Medusa

Production grade dagster project, with separate container adapting microservices:
- database container (using pg),
- webserver container,
- daemon container,
- grpc(s) container

This project is mono-container, but can be run separately

## Getting started

```bash
docker compose up -d --build
```

You can add as many grpc project as you want, just make sure to include it in `workspace.yaml`.

Each grpc container can have different library version or even different dependencies.

### Adding new Python dependencies

You can specify new Python dependencies in `requirements.txt`.

### Shared resources

Some resources are made global in 
`shared/resources`  
`shared/utils`  
`shared/partitions`  
so multiple grpc container can use them without repeating the code (DRY).

### Unit testing

TBD

### Schedules and sensors

### Partitions

### 