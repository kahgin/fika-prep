import os
import google.generativeai as genai

from dotenv import load_dotenv
load_dotenv()
API_KEY = os.getenv("GOOGLE_AI_STUDIO_KEY")

if not API_KEY:
    raise ValueError("Please set the 'GOOGLE_AI_STUDIO_KEY' in your .env file.")

genai.configure(api_key=API_KEY)

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
    generation_config = {
        "temperature": 0.7,
        "max_output_tokens": max_tokens,
    }
    
    try:
        response = model.generate_content(question, generation_config=generation_config)
        return response.text
    except Exception as e:
        return f"Sorry, an error occurred: {e}"

if __name__ == "__main__":
    user_question = "What is machine learning in one sentence?"
    answer = ask_gemini(user_question)
    print(f"Q: {user_question}")
    print(f"A: {answer}\n")
    
    follow_up = "Now give me a simple example."
    answer_2 = ask_gemini(follow_up)
    print(f"Q: {follow_up}")
    print(f"A: {answer_2}")