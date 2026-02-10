module.exports = {
  apps: [
    {
      name: "datum-scan-all",
      script: "scripts/scanAll.js",
      interpreter: "node",
      autorestart: false,
      cron_restart: "*/10 * * * *",
      time: true,
      env: {
        NODE_ENV: "production",
      },
    },
  ],
};
