import google.auth
import google.auth.transport.urllib3
import urllib3
import datetime
import time

class TokenProvider:
    def __init__(self):
        self.credentials, _ = google.auth.default()
        self.http_client = urllib3.PoolManager()

    def valid_credentials(self):
        if not self.credentials.valid:
            self.credentials.refresh(google.auth.transport.urllib3.Request(self.http_client))
        return self.credentials

    def confluent_token(self):
        creds = self.valid_credentials()
        utc_expiry = creds.expiry.replace(tzinfo=datetime.timezone.utc)
        expiry_seconds = (utc_expiry - datetime.datetime.now(datetime.timezone.utc)).total_seconds()
        return creds.token, time.time() + expiry_seconds

def make_token():
    token_provider = TokenProvider()
    token, expiry_time = token_provider.confluent_token()
    return token

if __name__ == "__main__":
    token = make_token()
    print(f"Generated Token: {token}")
