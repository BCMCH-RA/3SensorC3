/*
  ============================================================================
  XIAO ESP32C3 + MPU6050  -  200 Hz IMU streamer over BLE (Web Bluetooth)
  ============================================================================
  DEPLOYMENT: flash this SAME sketch onto all 3 boards - one MPU6050 per
  board, one board per limb (Right Leg / Left Leg / Torso).

  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  SET PER BOARD  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<
  Set to exactly one of: "RIGHT_LEG", "LEFT_LEG", "TORSO"
*/
#define SENSOR_ROLE "RIGHT_LEG"
/*
  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  WIRING: MPU6050 VCC->3V3, GND->GND, SCL->D5(GPIO7), SDA->D4(GPIO6), AD0->GND.

  SENSOR-HEALTH LAYER (new)
  --------------------------------------------------------------------------
  Field data showed a real fault class this design didn't previously catch:
  the I2C read can "succeed" (report the expected byte count) while the
  MPU6050/bus is actually wedged, silently handing back STALE data - one
  board froze on a single repeated value for ~20s, another read exact
  zeros for ~20s. Not explainable by the sensor just sitting still (a live
  sensor's noise floor alone makes bit-identical repeats across many
  consecutive reads vanishingly unlikely).

  Three layers now guard against this:
    1. WHO_AM_I verification (register 0x75, must read 0x68) before
       sampling starts, and again if a fault is detected - confirms the
       chip is actually responding, not just that the bus transaction
       nominally completed.
    2. Stuck-data detection: the raw 14 I2C bytes are compared to the
       previous read. STUCK_THRESHOLD identical reads in a row (or
       FAIL_THRESHOLD outright I2C failures) triggers I2C bus recovery
       (manual SCL clocking to free a wedged slave + STOP condition) and
       MPU6050 re-init, rate-limited by RECOVERY_COOLDOWN_MS. Frozen
       samples are NOT pushed to the ring buffer/BLE stream.
    3. A Status characteristic (NOTIFY) reports [sensorOk, recoveryCount]
       to the app whenever it changes, so a fault is visible live instead
       of only discoverable afterward in the CSV.

  RELIABILITY / BATTERY ARCHITECTURE (unchanged)
  --------------------------------------------------------------------------
  Same ring-buffer + batched-packet + ACK/resend ARQ design as before (see
  earlier revisions' comments): 10 samples/packet, sliding-window ACK,
  fast BLE connection interval, MPU6050 sleep + timer detach when idle.

  REQUIRED LIBRARY: NimBLE-Arduino (h2zero), v2.x API.
  ============================================================================
*/

#include <NimBLEDevice.h>
#include <Wire.h>
#include <Ticker.h>

// ------------------------- Configuration -----------------------------
#define SAMPLE_RATE_HZ        200
#define SAMPLE_INTERVAL_MS    (1000 / SAMPLE_RATE_HZ)   // 5 ms
#define BATCH_SIZE             10        // samples per BLE packet -> 20 pkt/s
#define RING_BUFFER_SAMPLES    1000      // ~5 s last-resort buffering headroom
#define MPU_ADDR                0x68
#define MPU_WHOAMI_REG           0x75
#define MPU_WHOAMI_EXPECTED      0x68

#define I2C_SDA_PIN               6      // XIAO ESP32C3 D4
#define I2C_SCL_PIN               7      // XIAO ESP32C3 D5

#define WINDOW_SIZE              8       // max packets in flight, unacked (~400ms)
#define RESEND_TIMEOUT_MS        150     // resend outstanding window if not acked in time

#define STUCK_THRESHOLD           10     // identical raw reads in a row -> fault (50ms)
#define FAIL_THRESHOLD             5     // outright I2C read failures in a row -> fault (25ms)
#define RECOVERY_COOLDOWN_MS      500    // min gap between recovery attempts

// BLE UUIDs
#define SERVICE_UUID          "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
#define TIME_SYNC_CHAR_UUID   "6e400002-b5a3-f393-e0a9-e50e24dcca9e" // write: 8B epoch ms (UTC, LE)
#define CONTROL_CHAR_UUID     "6e400003-b5a3-f393-e0a9-e50e24dcca9e" // write: 0x01 start / 0x00 stop
#define IMU_DATA_CHAR_UUID    "6e400004-b5a3-f393-e0a9-e50e24dcca9e" // notify: batched IMU packets
#define ACK_CHAR_UUID          "6e400005-b5a3-f393-e0a9-e50e24dcca9e" // write (no rsp): 2B highest contiguous seq received (LE)
#define ROLE_CHAR_UUID          "6e400006-b5a3-f393-e0a9-e50e24dcca9e" // read: ASCII role string
#define STATUS_CHAR_UUID         "6e400007-b5a3-f393-e0a9-e50e24dcca9e" // notify/read: [sensorOk(1B), recoveryCount(1B)]

const uint64_t IST_OFFSET_MS = 19800000ULL; // +5 hours 30 minutes

// ------------------------- Data structures -----------------------------
struct Sample {
  uint32_t elapsedMs;
  int16_t  ax, ay, az;
  int16_t  gx, gy, gz;
};

Sample ringBuf[RING_BUFFER_SAMPLES];
volatile uint16_t rbHead  = 0;
volatile uint16_t rbTail  = 0;
volatile uint16_t rbCount = 0;

volatile bool sampleFlag = false;
Ticker sampleTicker;

bool deviceConnected = false;
bool recording       = false;
bool timeSynced       = false;

uint64_t epochBaseMs   = 0;
uint32_t syncElapsedMs = 0;
uint64_t istEpochBaseMs = 0;

uint16_t nextSeq          = 0;
uint16_t oldestUnackedSeq = 0;
uint16_t resendCursor      = 0;
bool     resending          = false;
uint32_t lastSendMs         = 0;

// --- sensor health state ---
bool     sensorOk           = false;
uint8_t  recoveryAttempts   = 0;
uint8_t  lastRaw[14]        = {0};
bool     haveLastRaw        = false;
uint16_t stuckCount         = 0;
uint16_t failCount          = 0;
uint32_t lastRecoveryMs     = 0;

NimBLEServer*          pServer      = nullptr;
NimBLECharacteristic*  pImuChar     = nullptr;
NimBLECharacteristic*  pTimeChar    = nullptr;
NimBLECharacteristic*  pControlChar = nullptr;
NimBLECharacteristic*  pAckChar     = nullptr;
NimBLECharacteristic*  pRoleChar    = nullptr;
NimBLECharacteristic*  pStatusChar  = nullptr;

inline int16_t seqDiff(uint16_t a, uint16_t b) { return (int16_t)(a - b); }

// ------------------------- I2C bus recovery -----------------------------
// Standard I2C bus-recovery procedure: manually clock SCL up to 9 times to
// free a slave that's holding SDA low mid-transaction, then issue a STOP
// condition, then hand control back to the Wire peripheral.
void i2cBusRecovery() {
  Wire.end();
  pinMode(I2C_SCL_PIN, OUTPUT);
  pinMode(I2C_SDA_PIN, INPUT_PULLUP);
  digitalWrite(I2C_SCL_PIN, HIGH);
  delayMicroseconds(5);

  for (int i = 0; i < 9; i++) {
    if (digitalRead(I2C_SDA_PIN)) break; // bus released
    digitalWrite(I2C_SCL_PIN, LOW);
    delayMicroseconds(5);
    digitalWrite(I2C_SCL_PIN, HIGH);
    delayMicroseconds(5);
  }

  // Manual STOP: SDA low->high while SCL held high
  pinMode(I2C_SDA_PIN, OUTPUT);
  digitalWrite(I2C_SDA_PIN, LOW);
  delayMicroseconds(5);
  digitalWrite(I2C_SCL_PIN, HIGH);
  delayMicroseconds(5);
  digitalWrite(I2C_SDA_PIN, HIGH);
  delayMicroseconds(5);

  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  Wire.setClock(400000);
}

// ------------------------- MPU6050 low-level I/O -----------------------------
void mpuWrite(uint8_t reg, uint8_t val) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission();
}

bool mpuWhoAmI() {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(MPU_WHOAMI_REG);
  if (Wire.endTransmission(false) != 0) return false;
  Wire.requestFrom((uint8_t)MPU_ADDR, (uint8_t)1, (uint8_t)true);
  if (Wire.available() < 1) return false;
  return Wire.read() == MPU_WHOAMI_EXPECTED;
}

// Wakes + configures the MPU6050, verifying with WHO_AM_I (readable
// regardless of sleep state) rather than assuming register writes landed.
// Retries a few times before giving up.
bool mpuInitVerified() {
  for (int attempt = 0; attempt < 3; attempt++) {
    mpuWrite(0x6B, 0x00); // PWR_MGMT_1: wake up
    delay(10);
    if (mpuWhoAmI()) {
      mpuWrite(0x19, 0x04); // SMPLRT_DIV -> 200Hz sample rate
      mpuWrite(0x1A, 0x03); // CONFIG: DLPF ~44Hz
      mpuWrite(0x1B, 0x00); // GYRO_CONFIG: +/-250dps
      mpuWrite(0x1C, 0x00); // ACCEL_CONFIG: +/-2g
      return true;
    }
    delay(50);
  }
  return false;
}

void mpuSleep(bool sleep) {
  mpuWrite(0x6B, sleep ? 0x40 : 0x00);
}

bool mpuReadRawBytes(uint8_t raw[14]) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(0x3B); // ACCEL_XOUT_H
  if (Wire.endTransmission(false) != 0) return false;
  Wire.requestFrom((uint8_t)MPU_ADDR, (uint8_t)14, (uint8_t)true);
  if (Wire.available() < 14) return false;
  for (int i = 0; i < 14; i++) raw[i] = Wire.read();
  return true;
}

void IRAM_ATTR onSampleTick() { sampleFlag = true; }

// ------------------------- BLE status notify -----------------------------
void notifyStatus() {
  if (!pStatusChar) return;
  uint8_t payload[2] = { (uint8_t)(sensorOk ? 1 : 0), recoveryAttempts };
  pStatusChar->setValue(payload, 2);
  if (deviceConnected) pStatusChar->notify();
}

// ------------------------- Recovery trigger -----------------------------
void attemptRecovery() {
  uint32_t now = millis();
  if (now - lastRecoveryMs < RECOVERY_COOLDOWN_MS) return; // rate-limited
  lastRecoveryMs = now;

  Serial.println("[SENSOR] Fault detected (stuck/failed I2C reads) - attempting recovery...");
  i2cBusRecovery();
  bool ok = mpuInitVerified();

  bool wasOk = sensorOk;
  sensorOk = ok;
  if (recoveryAttempts < 255) recoveryAttempts++;
  stuckCount = 0;
  failCount = 0;
  haveLastRaw = false;

  Serial.printf("[SENSOR] Recovery attempt #%u -> %s\n", recoveryAttempts, ok ? "OK" : "STILL FAILING");
  if (wasOk != sensorOk || true) notifyStatus(); // always report the attempt
}

void startSampling() {
  mpuSleep(false);
  delay(5);
  sensorOk = mpuInitVerified(); // pre-flight check before trusting this session's data
  notifyStatus();

  rbHead = rbTail = rbCount = 0;
  nextSeq = oldestUnackedSeq = resendCursor = 0;
  resending = false;
  lastSendMs = millis();
  haveLastRaw = false;
  stuckCount = 0;
  failCount = 0;

  sampleTicker.attach_ms(SAMPLE_INTERVAL_MS, onSampleTick);
}

void stopSampling() {
  sampleTicker.detach();
  mpuSleep(true);
}

// ------------------------- BLE callbacks -----------------------------
class ServerCallbacks : public NimBLEServerCallbacks {
  void onConnect(NimBLEServer* srv, NimBLEConnInfo& info) override {
    deviceConnected = true;
    srv->updateConnParams(info.getConnHandle(), 6, 12, 0, 400); // fast interval: 7.5-15ms
    Serial.println("[BLE] Central connected - requested fast conn interval");
    notifyStatus(); // push current sensor health immediately
  }
  void onDisconnect(NimBLEServer* srv, NimBLEConnInfo& info, int reason) override {
    deviceConnected = false;
    recording = false;
    stopSampling();
    timeSynced = false;
    Serial.println("[BLE] Central disconnected -> re-advertising");
    NimBLEDevice::startAdvertising();
  }
};

class TimeSyncCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& info) override {
    std::string v = c->getValue();
    if (v.length() < 8) return;
    uint64_t epoch = 0;
    for (int i = 0; i < 8; i++) epoch |= ((uint64_t)(uint8_t)v[i]) << (8 * i);
    epochBaseMs    = epoch;
    syncElapsedMs  = millis();
    istEpochBaseMs = epochBaseMs + IST_OFFSET_MS;
    timeSynced     = true;
    Serial.printf("[TIME] Synced. UTC epoch ms=%llu  IST epoch ms=%llu\n",
                  (unsigned long long)epochBaseMs, (unsigned long long)istEpochBaseMs);
    uint8_t resp[8];
    for (int i = 0; i < 8; i++) resp[i] = (istEpochBaseMs >> (8 * i)) & 0xFF;
    c->setValue(resp, 8);
  }
};

class ControlCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& info) override {
    std::string v = c->getValue();
    if (v.length() < 1) return;
    uint8_t cmd = (uint8_t)v[0];
    if (cmd == 0x01) {
      if (!timeSynced) {
        Serial.println("[CTRL] Start requested but time not synced yet - ignoring");
        return;
      }
      recording = true;
      startSampling();
      Serial.println("[CTRL] Recording START");
    } else if (cmd == 0x00) {
      recording = false;
      stopSampling();
      Serial.println("[CTRL] Recording STOP");
    }
  }
};

class AckCallbacks : public NimBLECharacteristicCallbacks {
  void onWrite(NimBLECharacteristic* c, NimBLEConnInfo& info) override {
    std::string v = c->getValue();
    if (v.length() < 2) return;
    uint16_t ackSeq = (uint8_t)v[0] | ((uint16_t)(uint8_t)v[1] << 8);
    int16_t diff = seqDiff(ackSeq, (uint16_t)(oldestUnackedSeq - 1));
    if (diff <= 0) return;
    uint16_t inFlight = seqDiff(nextSeq, oldestUnackedSeq);
    uint16_t retiredPackets = (uint16_t)(ackSeq - oldestUnackedSeq) + 1;
    if (retiredPackets > inFlight) retiredPackets = inFlight;
    uint32_t retiredSamples = (uint32_t)retiredPackets * BATCH_SIZE;
    rbTail = (rbTail + retiredSamples) % RING_BUFFER_SAMPLES;
    rbCount -= retiredSamples;
    oldestUnackedSeq = (uint16_t)(oldestUnackedSeq + retiredPackets);
    lastSendMs = millis();
    if (resending && seqDiff(resendCursor, oldestUnackedSeq) < 0) {
      resendCursor = oldestUnackedSeq;
    }
  }
};

// ------------------------- Ring buffer helpers -----------------------------
void pushSample(const Sample &s) {
  if (rbCount >= RING_BUFFER_SAMPLES) {
    rbTail = (rbTail + 1) % RING_BUFFER_SAMPLES;
    rbCount--;
  }
  ringBuf[rbHead] = s;
  rbHead = (rbHead + 1) % RING_BUFFER_SAMPLES;
  rbCount++;
}

void readAndBufferSample() {
  uint8_t raw[14];

  if (!mpuReadRawBytes(raw)) {
    failCount++;
    stuckCount = 0; // distinct fault path from "stuck"; don't double-count
    if (failCount >= FAIL_THRESHOLD) attemptRecovery();
    return; // never push on a failed read
  }
  failCount = 0;

  bool stuck = haveLastRaw && (memcmp(raw, lastRaw, 14) == 0);
  if (stuck) {
    stuckCount++;
    if (stuckCount >= STUCK_THRESHOLD) attemptRecovery();
    return; // don't push a frozen/stale-duplicate sample
  }
  stuckCount = 0;
  memcpy(lastRaw, raw, 14);
  haveLastRaw = true;

  if (!sensorOk) { sensorOk = true; notifyStatus(); } // fresh data proves it's healthy again

  Sample s;
  s.elapsedMs = millis() - syncElapsedMs;
  s.ax = (int16_t)((raw[0]  << 8) | raw[1]);
  s.ay = (int16_t)((raw[2]  << 8) | raw[3]);
  s.az = (int16_t)((raw[4]  << 8) | raw[5]);
  // raw[6],raw[7] = temperature, unused
  s.gx = (int16_t)((raw[8]  << 8) | raw[9]);
  s.gy = (int16_t)((raw[10] << 8) | raw[11]);
  s.gz = (int16_t)((raw[12] << 8) | raw[13]);

  pushSample(s);
}

// ------------------------- Packet transmission -----------------------------
uint8_t packetBuf[7 + BATCH_SIZE * 13];

bool sendPacketAt(uint16_t ringStart, uint16_t seq) {
  uint32_t baseElapsed = ringBuf[ringStart].elapsedMs;
  packetBuf[0] = seq & 0xFF;
  packetBuf[1] = (seq >> 8) & 0xFF;
  packetBuf[2] = BATCH_SIZE;
  packetBuf[3] = baseElapsed & 0xFF;
  packetBuf[4] = (baseElapsed >> 8) & 0xFF;
  packetBuf[5] = (baseElapsed >> 16) & 0xFF;
  packetBuf[6] = (baseElapsed >> 24) & 0xFF;

  int off = 7;
  for (int i = 0; i < BATCH_SIZE; i++) {
    Sample &s = ringBuf[(ringStart + i) % RING_BUFFER_SAMPLES];
    uint32_t delta = s.elapsedMs - baseElapsed;
    uint8_t msOff = (uint8_t)(delta > 255 ? 255 : delta);
    packetBuf[off++] = msOff;
    int16_t vals[6] = { s.ax, s.ay, s.az, s.gx, s.gy, s.gz };
    for (int k = 0; k < 6; k++) {
      packetBuf[off++] = vals[k] & 0xFF;
      packetBuf[off++] = (vals[k] >> 8) & 0xFF;
    }
  }

  pImuChar->setValue(packetBuf, off);
  return pImuChar->notify();
}

void trySendPacket() {
  if (!deviceConnected || !recording) return;
  uint16_t inFlight = (uint16_t)seqDiff(nextSeq, oldestUnackedSeq);

  if (inFlight > 0 && (millis() - lastSendMs > RESEND_TIMEOUT_MS)) {
    resending = true;
    resendCursor = oldestUnackedSeq;
  }

  if (resending) {
    if (resendCursor == nextSeq) {
      resending = false;
    } else {
      uint16_t offsetPackets = (uint16_t)seqDiff(resendCursor, oldestUnackedSeq);
      uint32_t ringStart = (rbTail + (uint32_t)offsetPackets * BATCH_SIZE) % RING_BUFFER_SAMPLES;
      if (sendPacketAt(ringStart, resendCursor)) {
        resendCursor++;
        lastSendMs = millis();
      }
      return;
    }
  }

  if (inFlight < WINDOW_SIZE) {
    uint32_t inFlightSamples = (uint32_t)inFlight * BATCH_SIZE;
    uint32_t availableNew = rbCount - inFlightSamples;
    if (availableNew >= BATCH_SIZE) {
      uint32_t ringStart = (rbTail + inFlightSamples) % RING_BUFFER_SAMPLES;
      if (sendPacketAt(ringStart, nextSeq)) {
        nextSeq++;
        lastSendMs = millis();
      }
    }
  }
}

// ------------------------- Setup / Loop -----------------------------
void setup() {
  Serial.begin(115200);
  delay(200);

  Wire.begin(I2C_SDA_PIN, I2C_SCL_PIN);
  Wire.setClock(400000);
  sensorOk = mpuInitVerified();
  Serial.printf("[SENSOR] Initial WHO_AM_I check: %s\n", sensorOk ? "OK" : "FAILED");
  mpuSleep(true); // start asleep; wakes only while recording (battery saving)

  String bleName = String("XIAO-IMU-") + SENSOR_ROLE;

  NimBLEDevice::init(bleName.c_str());
  NimBLEDevice::setMTU(247);

  pServer = NimBLEDevice::createServer();
  pServer->setCallbacks(new ServerCallbacks());

  NimBLEService* pService = pServer->createService(SERVICE_UUID);

  pTimeChar = pService->createCharacteristic(
      TIME_SYNC_CHAR_UUID,
      NIMBLE_PROPERTY::WRITE | NIMBLE_PROPERTY::READ);
  pTimeChar->setCallbacks(new TimeSyncCallbacks());

  pControlChar = pService->createCharacteristic(
      CONTROL_CHAR_UUID,
      NIMBLE_PROPERTY::WRITE);
  pControlChar->setCallbacks(new ControlCallbacks());

  pImuChar = pService->createCharacteristic(
      IMU_DATA_CHAR_UUID,
      NIMBLE_PROPERTY::NOTIFY);

  pAckChar = pService->createCharacteristic(
      ACK_CHAR_UUID,
      NIMBLE_PROPERTY::WRITE_NR);
  pAckChar->setCallbacks(new AckCallbacks());

  pRoleChar = pService->createCharacteristic(
      ROLE_CHAR_UUID,
      NIMBLE_PROPERTY::READ);
  pRoleChar->setValue(SENSOR_ROLE);

  pStatusChar = pService->createCharacteristic(
      STATUS_CHAR_UUID,
      NIMBLE_PROPERTY::READ | NIMBLE_PROPERTY::NOTIFY);
  notifyStatus(); // sets initial value even before anyone's connected

  pService->start();

  NimBLEAdvertising* pAdv = NimBLEDevice::getAdvertising();
  pAdv->addServiceUUID(SERVICE_UUID);
  pAdv->setName(bleName.c_str());
  pAdv->start();

  Serial.print("[BLE] Advertising as '");
  Serial.print(bleName);
  Serial.println("' - waiting for connection...");
}

void loop() {
  if (sampleFlag) {
    sampleFlag = false;
    readAndBufferSample();
  }
  trySendPacket();
  if (!sampleFlag && rbCount == 0) {
    delay(1);
  }
}
