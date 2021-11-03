from discord_webhook import DiscordWebhook, DiscordEmbed
from os import listdir
from os.path import isfile, join


class Discord(object):
    def __init__(self, url):
        self.webhook = DiscordWebhook(url)
        self.dirname = ""

    def setEmbed(self):
        embed = DiscordEmbed(
            title="ETL Reporting", description="from kubernetes", color="03b2f8"
        )
        embed.set_author(
            name="ETL Admin",
            url="https://github.com/lovvskillz",
            icon_url="https://avatars0.githubusercontent.com/u/14542790",
        )
        embed.set_timestamp()

        onlyfiles = [f for f in listdir(self.dirname) if isfile(join(self.dirname, f))]

        for data in onlyfiles:
            if data.split(".")[-1] == "csv":
                with open(self.dirname + "/" + data, "rb") as f:
                    self.webhook.add_file(file=f.read(), filename="result.csv")
                self.webhook.add_embed(embed)

    def run(self):
        self.setEmbed()
        self.webhook.execute()