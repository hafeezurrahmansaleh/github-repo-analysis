from dataclasses import dataclass, field, asdict
from urllib.parse import urlparse
from typing import Any, Dict


@dataclass
class Repository:
    name: str
    id: str
    created_at: str
    updated_at: str
    size: int
    pushed_at: str
    html_url: str
    stargazers_count: int
    language: str
    forks: int
    open_issues: int
    watchers: int
    owner: str = field(init=False, default=None)

    def __post_init__(self):
        try:
            url = urlparse(self.html_url)
            path_segments = url.path.split('/')
            if len(path_segments) >= 2:
                self.owner = path_segments[1]
            else:
                self.owner = None
        except Exception as e:
            print(f"Error parsing URL: {e}")
            self.owner = None

    @classmethod
    def from_json(cls, repo_node: Dict[str, Any]):
        return cls(
            name=repo_node.get("name"),
            id=str(repo_node.get("id")),
            created_at=repo_node.get("created_at"),
            updated_at=repo_node.get("updated_at"),
            size=repo_node.get("size"),
            pushed_at=repo_node.get("pushed_at"),
            html_url=repo_node.get("html_url"),
            stargazers_count=repo_node.get("stargazers_count"),
            language=repo_node.get("language"),
            forks=repo_node.get("forks"),
            open_issues=repo_node.get("open_issues"),
            watchers=repo_node.get("watchers")
        )

    def __str__(self):
        return (f"Repository(owner='{self.owner}', name='{self.name}', id='{self.id}', created_at='{self.created_at}', "
                f"updated_at='{self.updated_at}', size={self.size}, pushed_at='{self.pushed_at}', html_url='{self.html_url}', "
                f"stargazers_count={self.stargazers_count}, language='{self.language}', forks={self.forks}, "
                f"open_issues={self.open_issues}, watchers={self.watchers})")

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# Example usage
if __name__ == "__main__":
    # Example JSON node
    repo_node = {
        "name": "truth",
        "id": 1936771,
        "created_at": "2011-06-14T23:41:37Z",
        "updated_at": "2023-01-01T12:34:56Z",
        "size": 12345,
        "pushed_at": "2023-01-01T12:34:56Z",
        "html_url": "https://github.com/google/truth",
        "stargazers_count": 150,
        "language": "Java",
        "forks": 30,
        "open_issues": 5,
        "watchers": 150
    }

    repository = Repository.from_json(repo_node)
    print(repository)
    print(repository.to_dict())
