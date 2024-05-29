import requests


class GitHubAPIClient:
    GITHUB_API_URL = "https://api.github.com/orgs/{}/repos?per_page=1000&page=1"

    def fetch_github_data(self, org_name):
        api_url = self.GITHUB_API_URL.format(org_name)

        response = requests.get(api_url)

        if response.status_code == 200:
            return response.text
        else:
            raise Exception(f"Failed to fetch data from GitHub API. Response Code: {response.status_code}")


# Example usage:
if __name__ == "__main__":
    client = GitHubAPIClient()
    try:
        data = client.fetch_github_data("org_name_here")
        print(data)
    except Exception as e:
        print(e)
