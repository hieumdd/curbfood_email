import os
import json
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABCMeta, abstractmethod

from google.cloud import bigquery
import google.auth
import jinja2

credentials, project = google.auth.default(
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/bigquery",
    ]
)
BQ_CLIENT = bigquery.Client(credentials=credentials, project=project)
SENDER = "siddhantmehandru.developer@gmail.com"


class View(metaclass=ABCMeta):
    def __init__(self):
        pass

    def get(self):
        with open(f"query/{self.get_query()}.sql", "r") as f:
            query = f.read()
        results = BQ_CLIENT.query(query).result()
        rows = [dict(row.items()) for row in results]
        return rows

    @abstractmethod
    def get_query(self):
        raise NotImplementedError

    def transform(self, rows):
        rows = [self._transform(row) for row in rows]
        return rows

    @abstractmethod
    def _transform(self, row):
        raise NotImplementedError

    def _transform_metrics(self, metrics):
        value_transformer = lambda x: {
            "api": f"{round(x['api']):,}",
            "manual": f"{round(x['manual']):,}",
            "diff": f"{x['diff']:,}",
            "perc_diff": f"{x['perc_diff'] * 100:.2f} %",
        }
        metrics_transformer = lambda x: {
            "name": x["name"].capitalize(),
            "value": value_transformer(x["value"]),
        }
        return [metrics_transformer(metric) for metric in metrics]

    def run(self):
        rows = self.get()
        return self.transform(rows)


class StandardView(View):
    def __init__(self):
        super().__init__()

    def get_query(self):
        return "standard"

    def _transform(self, row):
        return {"metrics": self._transform_metrics(row["metrics"])}


class LocationView(View):
    def __init__(self):
        super().__init__()

    def get_query(self):
        return "location"

    def _transform(self, row):
        return {
            "location": row["location"],
            "metrics": self._transform_metrics(row["metrics"]),
        }


class LocationChannelView(View):
    def __init__(self):
        super().__init__()

    def get_query(self):
        return "location_channel"

    def _transform(self, row):
        return {
            "location": row["location"],
            "channel": row["channel"],
            "metrics": self._transform_metrics(row["metrics"]),
        }


class EmailAlert:
    def __init__(self):
        pass

    def get_views(self):
        view_names = ["standard", "location", "location_channel"]
        views = [StandardView(), LocationView(), LocationChannelView()]
        return dict(zip(view_names, [i.run() for i in views]))

    def compose_html(self, views):
        loader = jinja2.FileSystemLoader(searchpath="./views")
        env = jinja2.Environment(loader=loader)
        template = env.get_template("report.html.j2")
        html = template.render(views=views)
        return html

    def compose(self, receiver, html):
        message = MIMEMultipart("alternative")
        message["Subject"] = f"Weekly Data Sanity Check"
        message["From"] = SENDER
        message["To"] = receiver

        part1 = MIMEText("Expand for more", "plain")
        part2 = MIMEText(html, "html")
        message.attach(part1)
        message.attach(part2)
        return message

    def send_email(self, html):
        password = os.getenv("APP_PWD")
        port = 465
        smtp_server = "smtp.gmail.com"
        context = ssl.create_default_context()
        with open("emails.json", "r") as f, smtplib.SMTP_SSL(
            smtp_server, port, context=context
        ) as server:
            receivers = json.load(f)["emails"]
            server.login(SENDER, password)
            for receiver in receivers:
                message = self.compose(receiver, html)
                server.sendmail(SENDER, receiver, message.as_string())

    def run(self):
        views = self.get_views()
        # with open("test.json", "w") as f:
        #     json.dump(views, f)
        html = self.compose_html(views)
        self.send_email(html)
