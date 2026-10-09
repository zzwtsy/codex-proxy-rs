-- 隐私规则默认关闭，策略由 Core/Provider 校验与执行
alter table runtime_settings add column codex_privacy_policy_json text not null
    default '{"enabled":false,"onError":"skip_rule","rules":[]}'
    check (
        json_valid(codex_privacy_policy_json)
        and json_type(codex_privacy_policy_json) = 'object'
        and json_type(codex_privacy_policy_json, '$.enabled') in ('true', 'false')
        and json_extract(codex_privacy_policy_json, '$.onError') in ('skip_rule', 'reject_request')
        and json_type(codex_privacy_policy_json, '$.rules') = 'array'
        and json_type(codex_privacy_policy_json, '$.enabled') is not null
        and json_type(codex_privacy_policy_json, '$.onError') is not null
        and json_type(codex_privacy_policy_json, '$.rules') is not null
    );
