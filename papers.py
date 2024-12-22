import requests


def get_paper_info(paper_id):
    base_url = "https://api.semanticscholar.org/graph/v1/paper/"
    url = f"{base_url}{paper_id}"
    fields = "title,authors,venue,referenceCount,citationCount,influentialCitationCount,abstract,tldr"

    # Prepare query parameters
    params = {}
    if fields:
        params['fields'] = fields

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to fetch paper info: {e}")

if __name__ == "__main__":
    paper_id = "788811dbb728837208980cb78028d05d3ba0b1c1"

    try:
        paper_info = get_paper_info(paper_id)
        print(paper_info)
    except Exception as e:
        print(e)
