import os
from typing import List, Dict, Any

from openai import OpenAI


def get_openrouter_client() -> OpenAI:
    """
    Create an OpenAI client configured to call OpenRouter.
    """
    api_key = os.getenv("OPENROUTER_API_KEY")
    if not api_key:
        raise RuntimeError("OPENROUTER_API_KEY is not set in the environment")
    
    client = OpenAI(
        api_key=api_key,
        base_url="https://openrouter.ai/api/v1"
        
    )
    return client

def llm_chat(
    messages: List[Dict[str, Any]],
    model: str = "openai/gpt-4o-mini",
    
)-> str:
    """Simple wrapper 

    Args:
        messages (List[Dict[str, Any]]): _description_
        model (str, optional): _description_. Defaults to "openai/gpt-4o-mini".

    
    """
    client = get_openrouter_client()
    response = client.chat.completions.create(
        model=model,
        messages=messages,
    )
    
    return response.choices[0].message.content

    