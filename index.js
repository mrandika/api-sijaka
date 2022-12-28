require('dotenv').config();
const mqtt = require('mqtt')
const axios = require('axios')

// Setup broker
const BROKER_HOST = process.env.BROKER_HOST
const BROKER_PORT = process.env.BROKER_PORT
const BROKER_CLEAN_CONNECTION = process.env.BROKER_CLEAN_CONNECTION
const BROKER_TIMEOUT = process.env.BROKER_TIMEOUT
const BROKER_RECONNECT_PERIOD= process.env.BROKER_RECONNECT_PERIOD

// Setup listener
const LISTENER_METHOD = process.env.LISTENER_METHOD
const LISTENER_HOST = process.env.LISTENER_HOST
const LISTENER_PORT = process.env.LISTENER_PORT
const LISTENER_URL = `${LISTENER_METHOD}://${LISTENER_HOST}:${LISTENER_PORT}/api`

// Compute broker client and url
const BROKER_CLIENT_ID = `sijaka_client_${Math.random().toString(16).slice(3)}`
const BROKER_URL = `mqtt://${BROKER_HOST}:${BROKER_PORT}`

// Create connection
const client = mqtt.connect(BROKER_URL, {
    BROKER_CLIENT_ID,
    clean: BROKER_CLEAN_CONNECTION,
    connectTimeout: BROKER_TIMEOUT,
    reconnectPeriod: BROKER_RECONNECT_PERIOD,
})

// Set the topic
const topics = [
    'sijaka/train/+/origin',
    'sijaka/train/+/destination',
    'sijaka/train/+/location',

    'sijaka/station/+/schedule/+/depart_time',
    'sijaka/station/+/schedule/+/arrive_time',
]

post = function(path, topic, data) {
    const payload = {
        topic: topic,
        data: data
    }

    const header = {
        headers: {
            Accept: 'application/json',
            'X-SIJAKA-APIKEY': process.env.APP_API_KEY
        }
    } 

    axios.post(`${LISTENER_URL}/${path}`, payload, header)
        .then((response) => {
            if (response.status === 200) {
                console.log(`[HTTP] Topic ${topic} with value ${data} posted to the listener.`)
            } else {
                console.error(`[HTTP] Failed to post the data! The status code is: ${response.status}.`)
                console.error(response)
            }
        }).catch((error) => {
            console.error(`[HTTP] ${error}`)
        })
}

client.on('connect', () => {
    console.log('[MQTT] Connected to broker!')
  
    client.subscribe(topics, () => {
      console.log(`[MQTT] Subscribed to topic '${topics}'!`)
    })
})
  
client.on('message', (topic, payload) => {
    console.log(`[MQTT] Received message at ${topic} that says ${payload}`)
    console.log(`[MQTT] Requesting HTTP request to listener...`)
  
    post(topic, topic, payload.toString())
})