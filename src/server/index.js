// @flow

import compression from 'compression'
import express from 'express'
import path from 'path'
import { Server } from 'http'

import { WEB_PORT, STATIC_PATH } from '../shared/config'
import { isProd } from '../shared/util'

const app = express()
// flow-disable-next-line
const http = Server(app)

app.use(compression())
app.use(STATIC_PATH, express.static('public'))

app.get('/auth', (req, res) => {
  res.sendFile(path.join(__dirname, '/pages/add_to_slack.html'))
})

app.get('/installation', (req, res) => {
  res.sendFile(path.join(__dirname, '/pages/add_to_slack.html'))
})

http.listen(WEB_PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Server running on port ${WEB_PORT} ${isProd ? '(production)' :
    '(development).\nKeep "yarn dev:wds" running in an other terminal'}.`)
})
