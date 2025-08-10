import os
import time
import asyncio
import requests
from mcp.client.session import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from conversation import Conversation

MCP_URL = os.environ.get("MCP_URL", "http://localhost:8080/mcp")
TASK_PROMPT_NAME = os.environ.get("TASK_PROMPT_NAME", "complete_tasks_prompt")


async def perform_tasks():
    async with streamablehttp_client(MCP_URL) as (read_stream, write_stream, _):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()

            # Get prompt
            prompt_res = await session.get_prompt(TASK_PROMPT_NAME)
            prompt = prompt_res.messages[0].content.text  # type: ignore

            # Get tools + clean schemas
            tools = (await session.list_tools()).tools
            tools_json = []
            for tool in tools:
                schema = tool.inputSchema
                if isinstance(schema, dict):
                    schema = {
                        k: v for k, v in schema.items()
                        if k not in ("additionalProperties", "$schema", "title")
                    }
                tools_json.append({
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": schema
                })

            # Conversation loop
            convo = Conversation(tools=tools_json)
            res = convo.add_message(prompt)
            tool_call = res["tool_call"]

            while tool_call:
                # Call tool
                result = await session.call_tool(tool_call.name, tool_call.input)

                # Serialize content
                clean_content = []
                for block in result.content:
                    content = block.model_dump()
                    content.pop("annotations", None)
                    content.pop("meta", None)
                    clean_content.append(content)

                res = convo.add_tool_result(tool_call.id, clean_content)
                tool_call = res["tool_call"]


def ping(url: str) -> bool:
    try:
        requests.get(url)
        return True
    except requests.exceptions.RequestException:
        return False


async def run_loop():
    waiting_secs = 0.0
    while True:
        start_time = time.time()
        if not ping(MCP_URL):
            print("\033[A\033[K" + f"Waiting for MCP server... ({int(waiting_secs)}s)")
            await asyncio.sleep(1)
            waiting_secs += time.time() - start_time
        else:
            print("MCP server running, performing tasks...")
            await perform_tasks()
            waiting_secs = 0.0


if __name__ == "__main__":
    asyncio.run(run_loop())
