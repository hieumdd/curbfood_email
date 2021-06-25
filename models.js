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

const EmailAlert = {
  get = async () => {
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
    channel: row.channel,
    metrics: row.metrics.map((metric) => ({
      name: metric.name.toUppercase(),
      value: metric.value.map((x) => ({
        api: x.api.toLocaleString(),
        manual: x.manual.toLocaleString(),
        diff: x.diff.toLocaleString(),
        perc_diff: (x.perc_diff * 100).toLocaleString({
          minimumFractionDigits: 2,
        }),
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
      to: "hieumdd@gmail.com, jhamb285@gmail.com",
      subject: "Hello ✔",
      text: "Hello world?",
      html,
    });
  
    return {messageSent: info.messageId};
  };

  const run = async () => {
    const rows = await get();
    const transformedRows = transform(rows);
    const html = await compose(transformedRows);
    await sendEmail(html);
  };
}

module.exports = EmailAlert;
