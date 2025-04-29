from dagster import EnvVar


API_LIST_PROJECTS = {
    "clipan_duitcair": {
        "login_endpoint": "https://api.astraworld.info/clipan/api/v2/auth/login",
        "username": EnvVar('API_USER').get_value(),
        "password": EnvVar('API_CLIPAN').get_value(),
    },
    "tam_concierge": {
        "login_endpoint": "https://api.astraworld.info/tam/api/v2/auth/login",
        "username": EnvVar('API_USER').get_value(),
        "password": EnvVar('API_TAM').get_value(),
    },
    "jmfi_mycash": {
        "login_endpoint": "https://api-jmfi.astraworld.info/api/v1/auth/login",
        "username": EnvVar('API_USER').get_value(),
        "password": EnvVar('API_JMFI').get_value(),
    },
}
