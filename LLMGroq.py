from groq import Groq
from io_vmm import get_env

class LLMGroq:
    def __init__(self, model="llama3-8b-8192"):
        self.client = Groq(api_key=get_env("GROQ_API_KEY"))
        self.model = model

    def query(self, content, role="user"):
        chat_completion = self.client.chat.completions.create(
            messages=[
                {
                    "role": role,
                    "content": content,
                }
            ],
            model=self.model,
        )

        return chat_completion.choices[0].message.content

if __name__ == "__main__":
    llm = LLMGroq()
    a = llm.query("What is the capital of france")
    print(a)