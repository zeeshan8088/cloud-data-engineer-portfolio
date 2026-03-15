# src/llm_summarizer.py
# ----------------------------------------------------------
# Purpose: Send anomaly descriptions to Gemini and get back
#          plain-English explanations automatically.
#
# Uses Google AI Studio API key (simpler than Vertex AI,
# same Gemini model, perfect for development & portfolio).
# ----------------------------------------------------------

import os
from google import genai
from dotenv import load_dotenv
load_dotenv()

# ── Configuration ─────────────────────────────────────────
# API key is read from environment variable — never hardcoded.
# Think of os.environ.get() like reading from a lockbox:
# the key is stored safely outside your code.
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

if not GEMINI_API_KEY:
    raise ValueError(
        "GEMINI_API_KEY environment variable not set. "
        "Run: $env:GEMINI_API_KEY = 'your-key-here'"
    )

# ── Initialise the GenAI client ───────────────────────────
# This time we use api_key= instead of vertexai=True.
# Same SDK, same Gemini model, different authentication path.
client = genai.Client(api_key=GEMINI_API_KEY)

# ── Model name ────────────────────────────────────────────
# gemini-2.0-flash: fast, capable, free tier available.
# This is the stable non-preview model — perfect for pipelines.
MODEL = "gemini-2.5-flash"
def get_anomaly_explanation(anomaly_description: str) -> str:
    """
    Takes a plain-English description of a data anomaly,
    sends it to Gemini, and returns a plain-English explanation.

    Args:
        anomaly_description: A string describing what's wrong
                             with a specific data row.

    Returns:
        A string containing Gemini's analysis and recommended action.
    """

    prompt = f"""You are a data quality assistant for an e-commerce platform.

Analyze this anomaly found in our orders dataset and explain in 2-3 sentences:
1. What likely caused it
2. What action a data engineer should take

Anomaly: {anomaly_description}

Be specific and concise. No bullet points — write in plain paragraph form."""

    response = client.models.generate_content(
        model=MODEL,
        contents=prompt
    )

    return response.text


# ── Test block ─────────────────────────────────────────────
if __name__ == "__main__":

    test_anomalies = [
        "Order ID ORD-1042 has an order amount of -850.00 INR and quantity of 1. Customer ID C-2291.",
        "Order ID ORD-2187 has a quantity of 9999 and amount of 0.00 INR. Product ID PROD-004.",
        "Order ID ORD-3301 has an order timestamp of 2087-11-22, which is in the future. Customer ID C-0042."
    ]

    print("=" * 60)
    print("AI QUALITY MONITOR — Gemini Anomaly Explainer")
    print("=" * 60)

    for anomaly in test_anomalies:
        print(f"\n📌 ANOMALY:\n{anomaly}")
        print(f"\n🤖 GEMINI SAYS:")
        explanation = get_anomaly_explanation(anomaly)
        print(explanation)
        print("-" * 60)