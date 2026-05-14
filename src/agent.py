"""
Spatial query agent for Australian city building data (Sydney, Melbourne, Brisbane, Perth).

Flow: (1) check_city_status(city) → (2) load_city_data(city) if needed →
(3) execute_query(SQL) with FROM buildings_{city}. View name is always buildings_{city} (e.g. buildings_sydney).
"""

from langchain.agents import create_agent
from langgraph.types import Command
from langgraph.checkpoint.memory import InMemorySaver
from langchain.chat_models import init_chat_model
from src.tools import tools
from dotenv import load_dotenv
from langchain.agents import AgentState
from src.prompts.system_prompt import *
from typing import Annotated, NotRequired, TypedDict, Any
from langchain.agents.middleware import (
    wrap_tool_call,
    AgentState,
    dynamic_prompt,
    ModelRequest
)
from langchain_core.messages import ToolMessage
from src.data_manager import get_city_info
from src.cloud_auth import ensure_aws_credentials_from_profile

load_dotenv()
ensure_aws_credentials_from_profile()


class CityInfoDict(TypedDict):
    """One city's status from check_city_status (supported, on_disk, in_spark, etc.)."""
    supported: Annotated[bool, "City is in CITY_CONFIGS."]
    in_spark: Annotated[bool, "City data is loaded in Spark this session (can query now)."]
    on_disk: Annotated[bool, "Enriched GeoParquet exists on disk (fast reload)."]
    needs_download: Annotated[bool, "Raw data not downloaded yet (slow path)."]
    view: NotRequired[Annotated[str | None, "Spark temp view name, e.g. buildings_sydney."]]
    count: NotRequired[Annotated[int | None, "Row count when in_spark."]]


class LoadedCityInfo(TypedDict):
    """Info for a city already loaded in Spark. Extend with more keys later if needed."""
    view: Annotated[str, "Spark temp view name, e.g. buildings_sydney."]
    count: Annotated[int, "Number of building rows in the view."]


def _merge_loaded_cities(
    current: dict[str, LoadedCityInfo] | None, update: dict[str, LoadedCityInfo]
) -> dict[str, LoadedCityInfo]:
    """Reducer: merge new city (or cities) into loaded_cities. Required because operator.add fails on dicts."""
    return {**(current or {}), **update}


class CustomState(AgentState):
    context_loaded: bool
    # Mirrors data_manager.get_loaded_cities(): {city_name: {"view", "count"}}; updated when load_city_data returns Command.
    loaded_cities: Annotated[dict[str, LoadedCityInfo], _merge_loaded_cities]

model = init_chat_model("gpt-4.1")

# Middleware to update the loaded_cities state
# @wrap_tool_call
# def city_update_middleware(request, handler):
#     print(f"🔧 TOOL: {request.tool_call['name']} | Args: {request.tool_call['args']}")
#     return handler(request)  # just pass through

@dynamic_prompt
def dynamic_prompt(request: ModelRequest) -> str:
    loaded_cities = request.state.get("loaded_cities", {})
    loaded_cities_str = "\n".join([f"- {city}: {info['view']} ({info['count']:,} rows)" for city, info in loaded_cities.items()])
    system_prompt = SYSTEM_PROMPT.format(loaded_cities_str=loaded_cities_str)

    # print("Dynamicsystem_prompt:", system_prompt)
    return system_prompt  
# Create the agent with skill support
agent = create_agent(
    model,
    system_prompt=SYSTEM_PROMPT,
    middleware=[dynamic_prompt], 
    state_schema=CustomState, 
    tools=tools,
    checkpointer=InMemorySaver(),
)

from typing import Optional
import uuid
from datetime import datetime

# Configuration for this conversation thread
thread_id = str(uuid.uuid4())
config = {"configurable": {"thread_id": thread_id}}

def run_query(query: str, config: Optional[dict] = config, **kwargs):
    """Invoke the agent with a user message. Pass config= for thread config, and **kwargs for extra state (e.g. loaded_cities)."""
    start_time = datetime.now()
    result = agent.invoke(
        {"messages": [{"role": "user", "content": query}], **kwargs},
        config,
    )

    end_time = datetime.now()
    print(f"Time taken: {end_time - start_time}")
    # Print the conversation
    for message in result["messages"]:
        if hasattr(message, 'pretty_print'):
            message.pretty_print()
        else:
            print(f"{message.type}: {message.content}")
            
    