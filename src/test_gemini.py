import os
import google.generativeai as genai

# --- Setup: Load your API Key ---
from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("GOOGLE_AI_STUDIO_KEY")

if not API_KEY:
    raise ValueError("Please set the 'GOOGLE_AI_STUDIO_KEY' in your .env file.")

# Configure the Gemini library with your key
genai.configure(api_key=API_KEY)

# Choose a model (gemini-2.5-flash is fast and free-tier eligible)
MODEL_NAME = "gemini-2.5-flash"
model = genai.GenerativeModel(MODEL_NAME)

def ask_gemini(question: str, max_tokens: int = 100) -> str:
    """
    Sends a question to Gemini and returns a short reply.
    
    Args:
        question: The user's question string.
        max_tokens: Limits the response length for a short answer.
    
    Returns:
        The model's text reply.
    """
    # Configure the API call for a short, deterministic reply
    generation_config = {
        "temperature": 0.7,  # Controls randomness. 0.0 is deterministic, 1.0 is creative.
        "max_output_tokens": max_tokens,  # Hard limit on response length.
    }
    
    try:
        # Send the prompt and get the response
        response = model.generate_content(question, generation_config=generation_config)
        # Return the text from the response
        return response.text
    except Exception as e:
        # Basic error handling
        return f"Sorry, an error occurred: {e}"

# --- Example: How to use it ---
if __name__ == "__main__":
    # Example 1: A simple greeting question
    user_question = "What is machine learning in one sentence?"
    answer = ask_gemini(user_question)
    print(f"Q: {user_question}")
    print(f"A: {answer}\n")
    
    # Example 2: You can simulate a chat by calling the function repeatedly
    follow_up = "Now give me a simple example."
    answer_2 = ask_gemini(follow_up)
    print(f"Q: {follow_up}")
    print(f"A: {answer_2}")