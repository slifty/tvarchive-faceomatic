// @flow

import compression from 'compression'
import express from 'express'
import path from 'path'
import fs from 'fs'
import request from 'request'
import dotenv from 'dotenv'
import { Server } from 'http'

import { WEB_PORT, STATIC_PATH } from '../shared/config'
import { isProd } from '../shared/util'

dotenv.config()

const app = express()
// flow-disable-next-line
const http = Server(app)

app.use(compression())
app.use(STATIC_PATH, express.static('public'))

app.get('/auth', (req, res) => {
  res.sendFile(path.join(__dirname, '/pages/add_to_slack.html'))
})

app.get('/auth/redirect', (req, res) => {
  const options = {
    uri: `https://slack.com/api/oauth.access?code=${req.query.code}&client_id=${process.env.SLACK_CLIENT_ID}&client_secret=${process.env.SLACK_CLIENT_SECRET}&redirect_uri=${process.env.SLACK_REDIRECT_URI}`,
    method: 'GET',
  }
  request(options, (error, response, body) => {
    const JSONresponse = JSON.parse(body)
    if (!JSONresponse.ok) {
      // eslint-disable-next-line no-console
      console.log(JSONresponse)
      res.send(`Error encountered: \n${JSON.stringify(JSONresponse)}`).status(200).end()
    } else {
      // eslint-disable-next-line no-console
      console.log(JSONresponse)

      const file = path.join(__dirname, '../../webhooks.txt')
      fs.appendFileSync(file, JSONresponse.incoming_webhook)
      res.send('Success!')
    }
  })
})

http.listen(WEB_PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Server running on port ${WEB_PORT} ${isProd ? '(production)' :
    '(development).\nKeep "yarn dev:wds" running in an other terminal'}.`)
})
