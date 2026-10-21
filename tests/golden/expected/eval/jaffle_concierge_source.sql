CREATE TABLE SST_REF_DEV.PROFILES.EVAL_SRC_JAFFLE_CONCIERGE_02D8AB4 (
    INPUT_QUERY VARCHAR NOT NULL
  , GROUND_TRUTH VARIANT NOT NULL
);

INSERT INTO SST_REF_DEV.PROFILES.EVAL_SRC_JAFFLE_CONCIERGE_02D8AB4 (INPUT_QUERY, GROUND_TRUTH)
SELECT
    'Which location took the most orders across the whole menu?'::VARCHAR AS INPUT_QUERY
  , PARSE_JSON('{"ground_truth_output":"The reply names the location with the most orders, from the analytics\\nagent''s answer.","ground_truth_subagents":["SST_REF_DEV.JAFFLE.JAFFLE_ANALYTICS_AGENT"]}')         AS GROUND_TRUTH
UNION ALL
SELECT
    'Has my jaffle order left the kitchen?'::VARCHAR AS INPUT_QUERY
  , PARSE_JSON('{"ground_truth_output":"The reply gives the order''s delivery status, from the delivery agent''s\\nanswer.","ground_truth_subagents":["SST_REF_DEV.JAFFLE.JAFFLE_DELIVERY_AGENT"]}')         AS GROUND_TRUTH
UNION ALL
SELECT
    'Which flour supplier delivers late most often?'::VARCHAR AS INPUT_QUERY
  , PARSE_JSON('{"ground_truth_output":"The reply declines: no agent of the profile holds supplier data.","ground_truth_subagents":[]}')         AS GROUND_TRUTH;
