//! 验证 Codex 增量工具声明覆盖、namespace 合并及响应 schema 同步

use super::*;
use provider_xai::GrokCanonicalDecoder;

fn declaration(tools: Vec<Value>) -> Value {
    json!({"type":"additional_tools", "role":"developer", "tools":tools})
}

fn function(name: &str, parameter_type: &str) -> Value {
    json!({"type":"function", "name":name, "parameters":{
        "type":"object", "properties":{"value":{"type":parameter_type}}
    }})
}

#[test]
fn namespace_updates_should_replace_members_and_preserve_undeclared_members() {
    let old = json!({"type":"namespace", "name":"workspace", "tools":[
        function("lookup", "integer"), function("read", "string")
    ]});
    let latest = json!({"type":"namespace", "name":"workspace", "tools":[
        function("lookup", "string"), function("write", "string")
    ]});
    let encoded = GrokResponsesRequest::encode(
        &raw_request(json!({"input":[declaration(vec![old]), declaration(vec![latest])]})),
        "grok-4.5",
        &client_key(),
    )
    .unwrap();
    let tools = encoded.body()["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 3);
    assert_eq!(tools[0]["name"], "workspace__lookup");
    assert_eq!(
        tools[0]["parameters"]["properties"]["value"]["type"],
        "string"
    );
    assert_eq!(tools[1]["name"], "workspace__read");
    assert_eq!(tools[2]["name"], "workspace__write");

    let created = json!({"type":"response.created", "response":{"id":"resp_incremental"}});
    let completed = json!({"type":"response.completed", "response":{
        "id":"resp_incremental", "status":"completed", "tools":tools, "output":[]
    }});
    let events = GrokCanonicalDecoder::for_request("grok-4.5", &encoded)
        .push(format!("data: {created}\n\ndata: {completed}\n\n").as_bytes())
        .unwrap();
    let restored = events
        .iter()
        .filter_map(|event| event.wire_event())
        .find_map(|wire| wire.data().pointer("/response/tools"))
        .unwrap();
    assert_eq!(
        restored,
        &json!([{"type":"namespace", "name":"workspace", "tools":[
            function("lookup", "string"), function("read", "string"), function("write", "string")
        ]}])
    );
}

#[test]
fn replaced_integer_schema_should_not_rewrite_new_number_arguments() {
    let encoded = GrokResponsesRequest::encode(
        &raw_request(json!({"tools":[function("lookup", "integer")], "input":[
            declaration(vec![function("lookup", "number")])
        ]})),
        "grok-4.5",
        &client_key(),
    )
    .unwrap();
    let event = json!({"type":"response.output_item.done", "output_index":0, "item":{
        "type":"function_call", "id":"fc_lookup", "call_id":"call_lookup",
        "name":"lookup", "arguments":"{\"value\":1.0}"
    }});
    let events = GrokCanonicalDecoder::for_request("grok-4.5", &encoded)
        .push(format!("data: {event}\n\n").as_bytes())
        .unwrap();
    let arguments = events
        .iter()
        .filter_map(|event| event.wire_event())
        .find_map(|wire| wire.data().pointer("/item/arguments"))
        .unwrap();
    assert_eq!(arguments, &json!("{\"value\":1.0}"));
}

#[test]
fn later_declaration_should_replace_the_tool_kind_before_alias_generation() {
    let encoded = GrokResponsesRequest::encode(
        &raw_request(json!({"input":[
            declaration(vec![function("lookup", "integer")]),
            declaration(vec![json!({"type":"custom", "name":"lookup"})])
        ]})),
        "grok-4.5",
        &client_key(),
    )
    .unwrap();
    let tools = encoded.body()["tools"].as_array().unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0]["name"], "lookup");
    assert_eq!(tools[0]["parameters"]["required"], json!(["input"]));
}

#[test]
fn tool_search_results_should_follow_the_same_declaration_order() {
    let latest = function("lookup", "number");
    let encoded = GrokResponsesRequest::encode(
        &raw_request(json!({"input":[
            declaration(vec![function("lookup", "integer")]),
            {"type":"tool_search_output", "call_id":"search_1", "execution":"server", "tools":[latest]}
        ]})), "grok-4.5", &client_key(),
    ).unwrap();
    assert_eq!(encoded.body()["tools"], json!([latest]));
}
