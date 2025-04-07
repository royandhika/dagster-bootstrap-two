from dagster import asset

@asset()
def dummy() -> str:
    print("This is a dummy asset for testing purposes")
    return "dummy_value"