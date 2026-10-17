"""Score one agent reply the way an unpublished eval run does, with no Snowflake evaluation run.

`tool_selection` scores which tools the agent called, exactly, against a question's ground
truth; `judge` fills the prompts an LLM judge scores an answer with, and reads its score back.
Nothing here calls Snowflake: the run supplies the reply and asks the judge.
"""
