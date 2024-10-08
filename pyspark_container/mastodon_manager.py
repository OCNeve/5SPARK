from mastodon import Mastodon

token = "Sl3zM4M4XPuG5GelIfK1JIZw7FCgeTDOBHdY3uaNotI"
class Mastodon_manager:
	def __init__(self):
		self.mastodon = Mastodon(
							access_token=token, 
							api_base_url="https://mastodon.social"
						)

