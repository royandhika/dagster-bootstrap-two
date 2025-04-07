from shared.utils.custom_function import create_external_asset


external = [
    create_external_asset("ecentrix_alpha", ["sources", "ecentrix_alpha_ecentrix_session_log"], "...", ["MySQL"]),
    create_external_asset("ecentrix_alpha", ["sources", "ecentrix_alpha_ecentrix_reference"], "...", ["MySQL"]),
    create_external_asset("ecentrix_bravo", ["sources", "ecentrix_bravo_ecentrix_session_log"], "...", ["MySQL"]),
    create_external_asset("ecentrix_bravo", ["sources", "ecentrix_bravo_ecentrix_reference"], "...", ["MySQL"]),
    create_external_asset("ecentrix_predictive", ["sources", "ecentrix_predictive_ecentrix_session_log"], "...", ["MySQL"]),
    create_external_asset("ecentrix_predictive", ["sources", "ecentrix_predictive_ecentrix_reference"], "...", ["MySQL"]),

    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_tms_prospect"], "...", ["MySQL"]),
    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_tms_prospect_detail"], "...", ["MySQL"]),
    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_tms_prospect_campaign_result"], "...", ["MySQL"]),
    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_tms_prospect_history_contact"], "...", ["MySQL"]),
    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_tms_prospect_time_frame"], "...", ["MySQL"]),
    create_external_asset("outbound_mrsdso", ["sources", "outbound_mrsdso_cc_master_category"], "...", ["MySQL"]),

    create_external_asset("outbound_aop", ["sources", "outbound_aop_tms_prospect"], "...", ["MySQL"]),
]