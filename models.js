require("dotenv").config();
const { BigQuery } = require("@google-cloud/bigquery");
const nunjucks = require("nunjucks");
const nodemailer = require("nodemailer");

const bqClient = new BigQuery({
  scopes: [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/drive",
  ],
});

const DATASET = "curb_tracking";

const get = async () => {
  const query = `SELECT * FROM ${DATASET}._EmailAlert`;
  const queryOptions = {
    query,
  };
  const [rows] = await bqClient.query(queryOptions);
  return rows;
};

const transform = (rows) =>
  rows.map((row) => ({
    location: row.location,
    l_metrics: row.l_metrics.map((lMetric) => ({
      name: lMetric.name.toUpperCase(),
      value: {
        api: lMetric.value.api.toLocaleString(),
        manual: lMetric.value.manual.toLocaleString(),
        diff: lMetric.value.diff.toLocaleString(),
        perc_diff: `${(lMetric.value.perc_diff * 100).toLocaleString({
          minimumFractionDigits: 2,
        })} %`,
      },
    })),
    channel: row.channel.map((cChannel) => ({
      channel: cChannel.channel,
      lc_metrics: cChannel.lc_metrics.map((lcMetrics) => ({
        name: lcMetrics.name.toUpperCase(),
        value: {
          api: lcMetrics.value.api.toLocaleString(),
          manual: lcMetrics.value.manual.toLocaleString(),
          diff: lcMetrics.value.diff.toLocaleString(),
          perc_diff: `${(lcMetrics.value.perc_diff * 100).toLocaleString({
            minimumFractionDigits: 2,
          })} %`,
        },
      })),
    })),
  }));

const compose = async (rows) => {
  nunjucks.configure("views", { autoescape: true });
  const html = nunjucks.render("reports.html", { rows });
  return html;
};

const sendEmail = async (html) => {
  const transporter = nodemailer.createTransport({
    service: "gmail",
    host: "smtp.gmail.com",
    port: 465,
    secure: true,
    auth: {
      user: "siddhantmehandru.developer@gmail.com",
      pass: process.env.APP_PWD,
    },
  });

  const info = await transporter.sendMail({
    from: '"Sid Dev" <siddhantmehandru.developer@gmail.com>',
    to: "hieumdd@gmail.com, jhamb285@gmail.com, data-team-aaaad74vtnh6jwsk7fg74f3ycy@vidi-corp-team.slack.com",
    subject: "API vs. Manual",
    text: "API vs. Manual",
    html,
  });

  return { messageSent: info.messageId };
};

const run = async () => {
  const rows = await get();
  const transformedRows = transform(rows);
  const html = await compose(transformedRows);
  const results = await sendEmail(html);
  return results;
};

module.exports = run;
