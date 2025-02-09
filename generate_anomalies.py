import os
import json
from groq import Groq

def extract_normal_conditions(description):
    prompt = f"""Generate exactly 5 normal activities from this scene description.
    
    Scene Description:
    {description}
    
    Strict output rules:
    - Return ONLY a JSON array
    - No markdown formatting
    - No explanations
    - No additional text
    - Each description must:
    * Start with "a photo of"
    * Use present continuous tense (-ing)
    * Avoid adjectives
    * No OR/AND statements
    * Use simple English
    * Maximum 50 characters
    * One activity per line

    Example format:
    [{{"description": "a photo of person walking through door"}}]
    
    Return only the JSON array, nothing else."""

    try:
        client = Groq(api_key='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')
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": "You are a helpful assistant that generates scene descriptions."},
                {"role": "user", "content": prompt}
            ],
            model="llama3-70b-8192",  # Using Groq's LLama2 model
            temperature=0.2,
        )
        result = response.choices[0].message.content
        result = clean_response(result)
        parsed_result = json.loads(result)
        validate_normal_conditions(parsed_result)
        return parsed_result
    except Exception as e:
        print(f"[ERROR] Groq API error: {str(e)}")
        return None

def clean_response(response):
    # Remove any possible markdown code block indicators
    response = response.replace('```json', '').replace('```', '')
    return response.strip()

def validate_normal_conditions(conditions):
    if not isinstance(conditions, list):
        raise ValueError("Response must be a list")
    for index, condition in enumerate(conditions):
        if "description" not in condition or not isinstance(condition["description"], str):
            raise ValueError(f"Invalid description in condition {index + 1}")
        if len(condition["description"]) < 10:
            raise ValueError(f"Description {index + 1} is too short")

if __name__ == "__main__":
    scene_description = "A person enters a building and sits on a chair."
    output = extract_normal_conditions(scene_description)
    if output:
        print(json.dumps(output, indent=2))