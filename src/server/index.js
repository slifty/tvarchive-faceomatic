// @flow

import compression from 'compression'
import express from 'express'
import path from 'path'
import fs from 'fs'
import request from 'request'
import dotenv from 'dotenv'
import schedule from 'node-schedule'
import moment from 'moment'
import { execSync } from 'child_process'
import csv from 'csv'
import { Server } from 'http'

import { WEB_PORT, STATIC_PATH } from '../shared/config'
import { isProd } from '../shared/util'

// Bake matroid in for now
let accessTokenExpiration
let accessToken

dotenv.config()

const app = express()
// flow-disable-next-line
const http = Server(app)

// Slack files
const webhooksFile = path.join(__dirname, '../../webhooks.txt')
const payloadsFile = path.join(__dirname, '../../payloads.txt')

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
      res.send(`Error encountered: \n${JSON.stringify(JSONresponse)}`).status(200).end()
    } else {
      // eslint-disable-next-line no-console
      fs.appendFileSync(webhooksFile, `${JSONresponse.incoming_webhook.url}\n\r`)
      fs.appendFileSync(payloadsFile, `${body}\n\r`)
      res.send('Success!')
    }
  })
})

function getPrograms(callback) {
  // Get a list of programs from the archive
  const options = {
    uri: 'https://archive.org/details/tv?weekshows&output=json',
    method: 'GET',
  }

  request(options, (error, response, body) => {
    try {
      const JSONresponse = JSON.parse(body)
      callback(JSONresponse)
    } catch (err) {
      console.log('ERR: Getting program IDs from the archive.')
    }
  })
}

// eslint-disable-next-line no-unused-vars
function generalRequestHandler(error, response, body) {

}

function parseProgramId(programId) {
  const parts = programId.split('_')
  const network = parts.shift()
  const date = parts.shift()
  const year = date.slice(0, 4)
  const month = date.slice(4, 6)
  const day = date.slice(6, 8)
  const time = parts.shift()
  const hour = time.slice(0, 2)
  const minute = time.slice(2, 4)
  const second = time.slice(4, 6)
  const program = parts.join('_')
  const datetime = new Date(`${year}-${month}-${day} ${hour}:${minute}:${second} UTC`)

  return {
    id: programId,
    network,
    airtime: datetime,
    program,
  }
}

function getPaths(programId) {
  return {
    unprocessedPath: path.join(__dirname, `../../programs/_${programId}.json`),
    processingPath: path.join(__dirname, `../../programs/~${programId}.json`),
    processedPath: path.join(__dirname, `../../programs/${programId}.json`),
    videoPath: path.join(__dirname, `../../videos/${programId}.mp4`),
    ffmpegOutputPath: path.join(__dirname, `../../videos/${programId}_ffmpeg.out`),
    matroidOutputPath: path.join(__dirname, `../../results/${programId}.json`),
    processedOutputPath: path.join(__dirname, `../../results/${programId}_processed.json`),
  }
}

function isRegistered(programId) {
  const paths = getPaths(programId)
  if (fs.existsSync(paths.unprocessedPath)
  || fs.existsSync(paths.processingPath)
  || fs.existsSync(paths.processedPath)) {
    return true
  }

  return false
}

function filterPrograms(programList) {
  // Cut out programs that:
  // 1) didn't happen in the past two weeks
  // 2) Aren't on one of the channels we track
  // 3) have already been downloaded

  const filteredList = []
  for (let i = 0; i < programList.length; i += 1) {
    const programId = programList[i]
    const program = parseProgramId(programId)
    const now = new Date()

    if ((Math.abs(now.getTime() - program.airtime.getTime()) <= 86400000 * 14)
     && (program.network === 'CNNW'
      || program.network === 'FOXNEWSW'
      || program.network === 'MSNBCW'
      || program.network === 'BBCNEWS')
     && (!isRegistered(programId))) {
      filteredList.push(program)
    }
  }
  return filteredList
}

function registerPrograms(programList) {
  for (let i = 0; i < programList.length; i += 1) {
    const program = programList[i]
    const paths = getPaths(program.id)
    fs.writeFileSync(paths.unprocessedPath, JSON.stringify(program))
  }
}

function downloadProgram(program, callback) {
  const paths = getPaths(program.id)
  const file = fs.createWriteStream(paths.videoPath)

  const options = {
    uri: `http://archive.org/download/${program.id}/${program.id}.mp4`,
    method: 'GET',
    headers: {
      Cookie: `logged-in-user=${process.env.ARCHIVE_USER_ID};logged-in-sig=${process.env.ARCHIVE_SIG}`,
    },
  }

  const stream = request(options).pipe(file)
  stream.on('finish', () => {
    callback(true)
  })
}

function splitProgram(program) {
  const paths = getPaths(program.id)
  const cmd = `${process.env.FFMPEG_PATH} -i "${paths.videoPath}" -acodec copy -f segment -segment_time 1200 -vcodec copy -reset_timestamps 1 -map 0 -segment_list ${paths.ffmpegOutputPath} ${paths.videoPath}_OUTPUT%d.mp4`

  const files = []
  try {
    execSync(cmd, {
      stdio: 'ignore',
    })
    const output = fs.readFileSync(paths.ffmpegOutputPath, 'utf8')
    const pieces = output.split('\n')
    for (let i = 0; i < pieces.length; i += 1) {
      const piece = pieces[i]
      if (piece.trim() !== '') {
        files.push(path.join(__dirname, `../../videos/${piece}`))
      }
    }
    fs.unlinkSync(paths.ffmpegOutputPath)
  } catch (err) {
    console.log(`  ERROR: Couldn't split program :: ${program.id}`)
  }
  return files
}

function getDuration(videoPath) {
  const cmd = `${process.env.FFPROBE_PATH} -i "${videoPath}" -show_entries format=duration -v quiet -of csv="p=0"`
  let output = 0
  try {
    output = execSync(cmd, {
      encoding: 'utf8',
    })
  } catch (err) {
    console.log(`  ERROR: Couldn't get duration :: ${videoPath}`)
  }
  return parseInt(output.trim(), 10)
}

function getAccessToken(callback) {
  if (Date() < accessTokenExpiration) {
    callback(accessToken)
    return
  }

  const options = {
    uri: 'https://www.matroid.com/api/0.1/oauth/token',
    method: 'POST',
    form: {
      client_id: process.env.MATROID_CLIENT_ID,
      client_secret: process.env.MATROID_CLIENT_SECRET,
      grant_type: 'client_credentials',
    },
  }

  request(options, (error, response, body) => {
    try {
      const JSONresponse = JSON.parse(body)
      if (JSONresponse.access_token) {
        // eslint-disable-next-line no-console
        accessToken = JSONresponse.access_token
        accessTokenExpiration = Date() + (JSONresponse.expires_in - 1000)
        callback(accessToken)
      }
    } catch (err) {
      console.log(`    ACCESS TOKEN ERROR: ${body}`)
      callback(accessToken)
    }
  })
}

function startMatroidProcessing(videoPath, callback) {
  getAccessToken((token) => {
    console.log(`    DETECTION SUBMITTED: ${videoPath}`)
    const options = {
      uri: `https://www.matroid.com/api/0.1/detectors/${process.env.MATROID_DETECTOR_ID}/classify_video`,
      method: 'POST',
      formData: {
        file: fs.createReadStream(videoPath),
      },
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
    request(options, (error, response, body) => {
      try {
        const JSONresponse = JSON.parse(body)
        if (JSONresponse.video_id) {
          console.log(`    DETECTION PROCESSED: ${videoPath} :: ${JSONresponse.video_id}`)
          callback(JSONresponse.video_id)
        } else {
          setTimeout(() => {
            startMatroidProcessing(videoPath, callback)
          }, 60000 + (Math.random() * 100000))
          console.log(`    DETECTION ERROR: ${videoPath} :: Body: ${body} :: Error: ${error}`)
        }
      } catch (err) {
        setTimeout(() => {
          startMatroidProcessing(videoPath, callback)
        }, 60000 + (Math.random() * 100000))
        console.log(`    DETECTION ERROR: ${videoPath} :: Body: ${body} :: Error: ${error}`)
      }
    })
  })
}

function getMatroidResults(matroidVideoId, callback) {
  getAccessToken((token) => {
    const options = {
      uri: `https://www.matroid.com/api/0.1/videos/${matroidVideoId}`,
      method: 'GET',
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }

    request(options, (error, response, body) => {
      try {
        const JSONresponse = JSON.parse(body)
        if (JSONresponse.classification_progress === undefined) {
          return
        }

        if (JSONresponse.classification_progress !== 100) {
          console.log(`    PROCESSING RESULTS: ${JSONresponse.classification_progress} :: ${matroidVideoId}`)

          setTimeout(() => {
            getMatroidResults(matroidVideoId, callback)
          }, 15000 + (Math.random() * 10000))
        } else {
          callback(JSONresponse)
        }
      } catch (err) {
        console.log(`    PROCESSING ERROR: ${matroidVideoId}::${err}`)
        setTimeout(() => {
          getMatroidResults(matroidVideoId, callback)
        }, 60000 + (Math.random() * 600000))
      }
    })
  })
}

// Function to push a program to Matroid
function runMatroid(videoPath, index, callback) {
  startMatroidProcessing(videoPath, (matroidVideoId) => {
    getMatroidResults(matroidVideoId, (results) => {
      // Package the results
      const finalResults = {
        duration: getDuration(videoPath),
        modelId: process.env.MATROID_DETECTOR_ID,
        results,
      }

      // Clean up the video slice
      fs.unlinkSync(videoPath)
      callback(finalResults, index)
    })
  })
}

function storeResults(fullResults, program) {
  // Write a file saving the results for future
  const paths = getPaths(program.id)
  fs.writeFileSync(paths.matroidOutputPath, JSON.stringify(fullResults))
}

function secondsToTime(seconds) {
  const s = (`00${seconds % 60}`).slice(-2)
  const m = (`00${Math.floor((seconds % 3600) / 60)}`).slice(-2)
  const h = Math.floor((seconds % 86400) / 3600)
  return `${h}:${m}:${s}`
}

function loadSlackWebhooks() {
  const webhooks = fs.readFileSync(webhooksFile, 'utf8')
  return webhooks.split('\n')
}

function announceResults(fullResults, program) {
  // Generate the results and send them to all the slack buddies
  console.log(`  RESULTS: ${program.id}`)
  const paths = getPaths(program.id)

  let cursor = 0
  const processedResults = {}

  // Loop through each segment
  for (let i = 0; i < fullResults.length; i += 1) {
    const fullResult = fullResults[i]
    const segment = fullResult.results
    const duration = fullResult.duration
    const labels = segment.label_dict
    const detections = segment.detections

    // Create empty buckets for all labels
    for (let j = 0; j < labels; j += 1) {
      const label = labels[j]
      if (processedResults[label] === undefined) {
        processedResults[label] = []
      }
    }

    // Loop through each second
    const detectedSeconds = Object.keys(detections)
    for (let j = 0; j < detectedSeconds.length; j += 1) {
      const second = detectedSeconds[j]
      const detection = detections[second]
      const adjustedSecond = parseInt(second, 10) + cursor

      // Loop through each face result
      const detectedLabelIds = Object.keys(detection)
      for (let k = 0; k < detectedLabelIds.length; k += 1) {
        const labelId = detectedLabelIds[k]
        const frame = detection[labelId]
        const label = labels[labelId]
        let maxScore = 0

        for (let l = 0; l < frame.length; l += 1) {
          const face = frame[l]
          maxScore = Math.max(maxScore, face.score)
        }

        // Make sure this label has a bucket for results
        if (processedResults[label] === undefined) {
          processedResults[label] = []
        }

        // Only count hits with more than 90% confidence
        if (maxScore > 90) {
          processedResults[label][adjustedSecond] = maxScore
        }
      }
    }
    cursor += parseInt(duration, 10)
  }

  // Store the processed results for debugging
  fs.writeFileSync(paths.processedOutputPath, JSON.stringify(processedResults))

  const programName = program.program.replace('_', ' ')
  const airMoment = moment(program.airtime)

  let finalString = ''
  finalString += '======================'
  finalString += `\n<https://archive.org/details/${program.id}|${program.network}, ${programName}, ${airMoment.utcOffset(-8).format('YYYY-MM-DD hh:mm A')} PST>`

  // This should become something more pluggable, but for now
  // this will determine the order and rendering name for each
  // face we track
  const displayTable = [
    {
      label: 'mcconnell',
      display: 'Mitch McConnell',
    },
    {
      label: 'pelosi',
      display: 'Nancy Pelosi',
    },
    {
      label: 'ryan',
      display: 'Paul Ryan',
    },
    {
      label: 'schumer',
      display: 'Chuck Schumer',
    },
    {
      label: 'trump',
      display: 'Donald Trump',
    },
    {
      label: 'mccain',
      display: 'Guest Face: John McCain',
    },
    {
      label: 'clinton_bill',
      display: 'Test Face: Bill Clinton',
    },
    {
      label: 'clinton_hillary',
      display: 'Test Face: Hillary Clinton',
    },
    {
      label: 'carter',
      display: 'Test Face: Jimmy Carter',
    },
    {
      label: 'bush_w',
      display: 'Test Face: George W Bush',
    },
    {
      label: 'bush_hw',
      display: 'Test Face: George HW Bush',
    },
    {
      label: 'romney',
      display: 'Test Face: Mitt Romney',
    },
    {
      label: 'obama',
      display: 'Test Face: Barack Obama',
    },
  ]

  for (let i = 0; i < displayTable.length; i += 1) {
    const label = displayTable[i].label
    const display = displayTable[i].display
    if (label in processedResults) {
      const results = processedResults[label]
      if (results.length === 0) {
        finalString += `\n:no_entry_sign: \`${display}\` Not Found`
      } else {
        finalString += `\n:white_check_mark: \`${display}\` Detected`

        let start = -1
        let end = -1

        const seconds = Object.keys(results)
        for (let j = 0; j < seconds.length; j += 1) {
          const second = seconds[j]
          if (start === -1) {
            start = second
            end = second + 1
          }

          // Allow gaps of up to 3 seconds
          if (second - end <= 3) {
            end = second
          } else {
            finalString += `\n * ${secondsToTime(start)} - ${secondsToTime(end)} <https://archive.org/details/${program.id}#start/${start}/end/${end}|(${end - start}s)>`
            start = second
            end = second
          }
        }
        if (start !== -1) {
          finalString += `\n * ${secondsToTime(start)} - ${secondsToTime(end)} <https://archive.org/details/${program.id}#start/${start}/end/${end}|(${end - start}s)>`
        }
      }
    }
  }

  const slackWebhooks = loadSlackWebhooks()
  for (let i = 0; i < slackWebhooks.length; i += 1) {
    const webhook = slackWebhooks[i]
    if (webhook.trim() !== '') {
      const options = {
        uri: webhook,
        method: 'POST',
        headers: {
          'Content-type': 'application/json',
        },
        json: {
          text: finalString,
        },
      }
      request(options, generalRequestHandler)
    }
  }
}

function processProgram(program) {
  const paths = getPaths(program.id)

  // Mark the program as processing
  try {
    fs.renameSync(paths.unprocessedPath, paths.processingPath)
  } catch (err) {
    console.log(`ERROR starting to process ${program}: ${err}`)
    return
  }

  console.log(`  Downloading ${program.id}`)
  downloadProgram(program, (success) => {
    if (!success) {
      // This didn't work, retry it in 10 minutes
      console.log(`  ERROR: Couldn't download video :: ${program.id}`)
      setTimeout(() => {
        fs.renameSync(paths.processingPath, paths.unprocessedPath)
      }, 600000)
      return
    }
    console.log(`  DOWNLOADED: Video downloaded :: ${program.id}`)
    const videos = splitProgram(program)
    const fullResults = []
    let counter = videos.length
    console.log(`  SPLIT: (${videos.length} total) :: ${program.id}`)
    const watchResults = (results, index) => {
      fullResults[index] = results
      counter -= 1
      console.log(`  PROCESSED: Index ${index} (${counter} remaining) :: ${program.id}`)
      if (counter === 0) {
        // All done
        storeResults(fullResults, program)
        announceResults(fullResults, program)
        fs.renameSync(paths.processingPath, paths.processedPath)

        // Clean up the video file
        fs.unlinkSync(paths.videoPath)
      }
    }
    for (let i = 0; i < videos.length; i += 1) {
      const video = videos[i]
      runMatroid(video, i, watchResults)
    }
  })
}

function getUnprocessedProgramIds() {
  const files = fs.readdirSync(path.join(__dirname, '../../programs/'))
  const unprocessedPrograms = []
  for (let i = 0; i < files.length; i += 1) {
    const file = files[i]
    if (file.slice(0, 1) === '_') {
      unprocessedPrograms.push(file.slice(1, -5))
    }
  }
  return unprocessedPrograms
}

function getProcessedProgramIds() {
  const files = fs.readdirSync(path.join(__dirname, '../../programs/'))
  const processedPrograms = []
  for (let i = 0; i < files.length; i += 1) {
    const file = files[i]
    if (file.slice(0, 1) !== '_'
     && file.slice(0, 1) !== '~'
     && file.slice(0, 1) !== '.') {
      processedPrograms.push(file.slice(0, -5))
    }
  }
  return processedPrograms
}

function getProcessedResultFiles() {
  const files = fs.readdirSync(path.join(__dirname, '../../results/'))
  const processedResultFiles = []
  for (let i = 0; i < files.length; i += 1) {
    const file = files[i]
    if (file.slice(-15) === '_processed.json') {
      processedResultFiles.push(file)
    }
  }
  return processedResultFiles
}

function getDisplayValues(label) {
  const displayTable = {
    mcconnell: {
      display: 'Mitch McConnell',
    },
    pelosi: {
      display: 'Nancy Pelosi',
    },
    ryan: {
      display: 'Paul Ryan',
    },
    schumer: {
      display: 'Chuck Schumer',
    },
    trump: {
      display: 'Donald Trump',
    },
    mccain: {
      display: 'John McCain',
    },
    clinton_bill: {
      display: 'Test Face: Bill Clinton',
    },
    clinton_hillary: {
      display: 'Test Face: Hillary Clinton',
    },
    carter: {
      display: 'Test Face: Jimmy Carter',
    },
    bush_w: {
      display: 'Test Face: George W Bush',
    },
    bush_hw: {
      display: 'Test Face: George HW Bush',
    },
    romney: {
      display: 'Test Face: Mitt Romney',
    },
  }

  if (label in displayTable) {
    return displayTable[label]
  }

  return {
    display: label,
  }
}

function generateClip(label, programId, start, end) {
  const program = parseProgramId(programId)
  const programName = program.program.split('_').join(' ')
  const airMoment = moment(program.airtime)
  const displayValues = getDisplayValues(label)
  airMoment.add(start, 'seconds')

  // Clips should be at least 1 second long
  if (start === end) {
    // eslint-disable-next-line no-param-reassign
    end = start + 1
  }

  const clip = {
    label,
    displayValues,
    network: program.network,
    program: programName,
    airtime: airMoment,
    date: `${airMoment.utcOffset(-8).format('YYYY-MM-DD')}`,
    time: `${airMoment.utcOffset(-8).format('hh:mm:ss A')} PST`,
    duration: end - start,
    programId: program.id,
    link: `https://archive.org/details/${program.id}/start/${start}/end/${end}`,
  }
  return clip
}

/* This utility function will determine if a given airing is  part of
   the program it was captured in, or if it is actually part of the one
   minute overlap that we sometimes add to the end of programs.

   This is determined by looking at the next program aired on the
   channel and seeing if the airtime overlaps.  If so, it is considered
   to be part of the next program.

   NOTE: sortedProgramIdList is expected to be sorted by program Id
   */
function isPartOfNextProgram(airtime, currentProgramId, sortedProgramIdList) {
  // Find the next program
  const currentProgramIndex = sortedProgramIdList.indexOf(currentProgramId)
  const nextProgramIndex = currentProgramIndex + 1

  // If there is no next program, then we know the answer
  if (nextProgramIndex >= sortedProgramIdList.length) {
    return false
  }

  const nextProgramId = sortedProgramIdList[nextProgramIndex]

  // Is this program the most recent one on the current network?
  const currentProgram = parseProgramId(currentProgramId)
  const nextProgram = parseProgramId(nextProgramId)
  if (currentProgram.network !== nextProgram.network) {
    return false
  }

  // Did this air after the beginning of the start of the next program
  if (moment(airtime) >= moment(nextProgram.airtime)) {
    return true
  }

  return false
}

function generateResultsCSV(filestem, debug) {
  // Load up all results
  const processedResultFiles = getProcessedResultFiles()

  // Create an output Files
  const csvPath = path.join(__dirname, `../../csvs/${filestem}.csv`)
  const csvFile = fs.createWriteStream(csvPath)
  const tsvPath = path.join(__dirname, `../../csvs/${filestem}.tsv`)
  const resultsPaths = [csvPath, tsvPath]
  const tsvFile = fs.createWriteStream(tsvPath)
  const columns = [
    'Label',
    'Name',
    'Network',
    'Program',
    'Air Date',
    'Air Time',
    'Duration',
    'Archive ID',
    'URL',
    'Model ID',
  ]

  // Set up the CSV Pipeline
  const csvStringifier = csv.stringify({
    header: true,
    columns,
  })
  csvStringifier.on('readable', () => {
    let data = null
    // eslint-disable-next-line no-cond-assign
    while (data = csvStringifier.read()) {
      csvFile.write(data)
    }
  })

  // Set up the TSV Pipeline
  const tsvStringifier = csv.stringify({
    header: true,
    columns,
    delimiter: '\t',
  })
  tsvStringifier.on('readable', () => {
    let data = null
    // eslint-disable-next-line no-cond-assign
    while (data = tsvStringifier.read()) {
      tsvFile.write(data)
    }
  })

  // Generate a sorted list of all processed program IDs
  const programIdList = processedResultFiles.map(fileName => fileName.slice(0, -15))
  const sortedProgramIdList = programIdList.sort()

  // Go through each item and append the results
  for (let i = 0; i < processedResultFiles.length; i += 1) {
    const programId = processedResultFiles[i].slice(0, -15)
    const paths = getPaths(programId)

    // Load the Matroid model used
    const rawData = fs.readFileSync(paths.matroidOutputPath, 'utf8')
    const rawResults = JSON.parse(rawData)
    let modelId = ''
    if (rawResults.length > 0
    && 'modelId' in rawResults[0]) {
      modelId = rawResults[0].modelId
    }

    // Load the results
    fs.readFile(paths.processedOutputPath, 'utf8', (err, data) => {
      const processedResults = JSON.parse(data)
      const outputLabels = [
        'mcconnell',
        'pelosi',
        'ryan',
        'schumer',
        'trump',
      ]
      if (debug) {
        outputLabels.push(
          'mccain',
          'obama',
          'clinton_bill',
          'clinton_hillary',
          'bush_hw',
          'bush_w',
          'romney',
          'carter',
        )
      }

      for (let j = 0; j < outputLabels.length; j += 1) {
        const label = outputLabels[j]
        if (label in processedResults) {
          const results = processedResults[label]
          let start = -1
          let end = -1
          for (let k = 0; k < results.length; k += 1) {
            const second = parseInt(k, 10)
            if (results[second] !== null) {
              if (start === -1) {
                start = second
                end = second + 1
              }

              // Allow gaps of up to 3 seconds
              if (second - end <= 3) {
                end = second
              } else {
                const clip = generateClip(label, programId, start, end)
                const row = [
                  clip.label,
                  clip.displayValues.display,
                  clip.network,
                  clip.program,
                  clip.date,
                  clip.time,
                  clip.duration,
                  clip.programId,
                  clip.link,
                  modelId,
                ]

                if (!isPartOfNextProgram(clip.airtime, clip.programId, sortedProgramIdList)) {
                  csvStringifier.write(row)
                  tsvStringifier.write(row)
                }

                start = -1
                end = -1
              }
            }
          }
          if (start !== -1) {
            // TODO: DRY
            const clip = generateClip(label, programId, start, end)
            const row = [
              clip.label,
              clip.displayValues.display,
              clip.network,
              clip.program,
              clip.date,
              clip.time,
              clip.duration,
              clip.programId,
              clip.link,
              modelId,
            ]
            if (!isPartOfNextProgram(clip.airtime, clip.programId, sortedProgramIdList)) {
              csvStringifier.write(row)
              tsvStringifier.write(row)
            }
          }
        }
      }
    })
  }

  return resultsPaths
}

function lookupProgramDuration(archiveId, callback) {
  const options = {
    uri: `https://archive.org/metadata/${archiveId}`,
    method: 'GET',
  }

  request(options, (error, response, body) => {
    try {
      const JSONresponse = JSON.parse(body)
      callback(JSONresponse.metadata.imagecount)
    } catch (err) {
      callback(-1)
      console.log(`ERROR GETTING DURATION: ${archiveId} || ${body}`)
    }
  })
}

function generateProgramCSV(filestem) {
  // Load up all results
  const processedProgramIds = getProcessedProgramIds()

  // Create an output Files
  const csvPath = path.join(__dirname, `../../csvs/${filestem}.csv`)
  const csvFile = fs.createWriteStream(csvPath)
  const tsvPath = path.join(__dirname, `../../csvs/${filestem}.tsv`)
  const tsvFile = fs.createWriteStream(tsvPath)

  const programsPaths = [csvPath, tsvPath]

  const columns = [
    'Program ID',
    'Network',
    'Air Time',
    'Program',
    'Duration',
  ]

  // Set up the CSV Pipeline
  const csvStringifier = csv.stringify({
    header: true,
    columns,
  })
  csvStringifier.on('readable', () => {
    let data = null
    // eslint-disable-next-line no-cond-assign
    while (data = csvStringifier.read()) {
      csvFile.write(data)
    }
  })

  // Set up the TSV Pipeline
  const tsvStringifier = csv.stringify({
    header: true,
    columns,
    delimiter: '\t',
  })
  tsvStringifier.on('readable', () => {
    let data = null
    // eslint-disable-next-line no-cond-assign
    while (data = tsvStringifier.read()) {
      tsvFile.write(data)
    }
  })

  const logProgram = (programId) => {
    const paths = getPaths(programId)
    fs.readFile(paths.processedPath, 'utf8', (err, data) => {
      let program = parseProgramId(programId)
      try {
        program = JSON.parse(data)
        fs.writeFileSync(paths.processedPath, JSON.stringify(program))
      } catch (e) {
        console.log(`Error loading ${programId} ${e})`)
      }

      if ('imageCount' in program
      && program.imageCount > 0) {
        const row = [
          program.id,
          program.network,
          program.airtime,
          program.program,
          program.imageCount,
        ]
        csvStringifier.write(row)
        tsvStringifier.write(row)
      } else {
        lookupProgramDuration(programId, (duration) => {
          program.imageCount = duration

          // Write the row
          const row = [
            program.id,
            program.network,
            program.airtime,
            program.program,
            program.imageCount,
          ]
          csvStringifier.write(row)
          tsvStringifier.write(row)

          // Update the program with the timestamp...
          fs.writeFileSync(paths.processedPath, JSON.stringify(program))
        })
      }
    })
  }

  for (let i = 0; i < processedProgramIds.length; i += 1) {
    const programId = processedProgramIds[i]
    logProgram(programId)
  }

  return programsPaths
}

function deployData(deployFile, targetFile) {
  // S3 documentation: https://github.com/vmbrasseur/IAS3API
  const s3Url = `http://s3.us.archive.org/faceomatic/${targetFile}`
  const options = {
    method: 'put',
    headers: {
      authorization: `LOW ${process.env.ARCHIVE_S3_ACCESS}:${process.env.ARCHIVE_S3_SECRET}`,
    },
    body: fs.readFileSync(deployFile, 'utf8'),
  }

  request(s3Url, options, (err, httpResponse, body) => {
    if (err) {
      console.log('Upload Error: ', err, body)
    } else {
      console.log(`Uploaded ${deployFile}`, body)
    }
  })
}

// Set up scheduled download of program IDs
schedule.scheduleJob('30 * * * *', () => {
  console.log('Checking for unprocessed programs...')
  getPrograms((programIds) => {
    const programList = filterPrograms(programIds)
    registerPrograms(programList)
  })
})

// Set up scheduled processing of programs
schedule.scheduleJob('* * * * *', () => {
  console.log('Processing new programs...')
  const programIds = getUnprocessedProgramIds()
  for (let i = 0; i < programIds.length; i += 1) {
    const programId = programIds[i]
    const paths = getPaths(programId)
    console.log(`Processing ${programId}`)
    fs.readFile(paths.unprocessedPath, (err, data) => {
      const program = JSON.parse(data)
      processProgram(program)
    })
  }
})

// Set up scheduled generation of the csv
schedule.scheduleJob('0 * * * *', () => {
  console.log('Generating latest results files...')
  const csvTime = Date.now()
  // Result CSVs
  const resultCsvName = `results_${csvTime}`
  const results = generateResultsCSV(resultCsvName)
  console.log('Deploying the latest dataset...')
  console.log(`Generated: csvs/${resultCsvName}.csv`)
  console.log(`Generated: csvs/${resultCsvName}.tsv`)

  // Debug results CSVs
  generateResultsCSV('results', true)

  // Program CSVs
  const programCsvName = `programs_${csvTime}`
  const programs = generateProgramCSV(programCsvName)
  console.log(`Generated: csvs/${programCsvName}.csv`)
  console.log(`Generated: csvs/${programCsvName}.tsv`)

  // TODO: make this less rigid
  setTimeout(() => {
    deployData(results[0], 'results.csv')
    deployData(results[1], 'results.tsv')
    deployData(programs[0], 'programs.csv')
    deployData(programs[1], 'programs.tsv')
  }, 600000)
})

generateResultsCSV('results')
generateProgramCSV('programs')

http.listen(WEB_PORT, () => {
  // eslint-disable-next-line no-console
  console.log(`Server running on port ${WEB_PORT} ${isProd ? '(production)' :
    '(development).\nKeep "yarn dev:wds" running in an other terminal'}.`)
})
