import time
from kafka import KafkaProducer
import requests

from producer.GitHubApiClient import GitHubAPIClient


class GitHubDataProducer:

    @staticmethod
    def read_org_names_from_file(file_name) -> list:
        with open(file_name, 'r') as file:
            organization_names = [line.strip() for line in file if line.strip()]
        return organization_names

    def main(self):
        print("########### GitHub Data Producer started running..........")

        # Configure Kafka producer
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Change to your Kafka bootstrap servers
            key_serializer=lambda x: x.encode('utf-8') if x else None,
            value_serializer=lambda x: x.encode('utf-8') if x else None
        )

        # Fetch data from GitHub API
        gitHubApiClient = GitHubAPIClient()

        file_name = "../files/organization_list.txt"
        try:
            organization_names = self.read_org_names_from_file(file_name)
        except IOError as e:
            print(f"Error reading organization names from file: {e}")
            return

        print("########## GitHub Data Producer is in step 2......")
        print("================================================")
        print(f"########## Found {len(organization_names)} organizations to process ##########")
        print("================================================")

        try:
            for org_name in organization_names:
                print(f"Processing organization: {org_name}")
                try:
                    json_data = gitHubApiClient.fetch_github_data(org_name)  # Fetch data from GitHub API
                    if not json_data:
                        raise ValueError(f"No data fetched for organization: {org_name}")
                except Exception as e:
                    print(f"Error fetching data for {org_name}: {e}")
                    continue

                # Ensure json_data is a valid string before sending to Kafka
                if json_data is None:
                    print(f"Skipping {org_name} as json_data is None")
                    continue

                print(
                    f"Fetched data for {org_name}: {json_data[:100]}...")  # Print first 100 characters of the data for debugging

                # Send data to Kafka topic
                producer.send('github-data-topic', value=json_data)
                print(
                    f"Processing {org_name} repositories is done....\n\n***** Waiting for 15 seconds to process next org...")

                # Sleep for 15 seconds
                time.sleep(15)
        except Exception as e:
            print(f"Unexpected error: {e}")
        finally:
            # Close the producer
            producer.close()


if __name__ == "__main__":
    gtp = GitHubDataProducer()
    gtp.main()
