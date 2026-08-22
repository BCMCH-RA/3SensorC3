/*
  ============================================================================
  XIAO ESP32C3 + MPU6050  -  200 Hz IMU streamer over BLE (Web Bluetooth)
  ============================================================================
  DEPLOYMENT: flash this SAME sketch onto all 3 boards - one MPU6050 per
  board, one board per limb (Right Leg / Left Leg / Torso). Each board is
  an independent BLE peripheral with its own connection, its own clock
  sync, and its own ARQ/sequence state; the phone/PC app connects to all
  three separately and merges the streams by timestamp.

  THE ONLY LINE YOU NEED TO CHANGE PER BOARD is SENSOR_ROLE below.
  ============================================================================

  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  SET PER BOARD  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<
  Set to exactly one of: "RIGHT_LEG", "LEFT_LEG", "TORSO"
*/
#define SENSOR_ROLE "RIGHT_LEG"
/*
  >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>

  WIRING (per board, XIAO ESP32C3 <-> MPU6050)
  --------------------------------------------------------------------------
  MPU6050 VCC -> 3V3   MPU6050 GND -> GND
  MPU6050 SCL -> D5 (GPIO7, default I2C SCL)   MPU6050 SDA -> D4 (GPIO6, default I2C SDA)
  MPU6050 AD0 -> GND   (I2C address 0x68)

  IDENTITY
  --------------------------------------------------------------------------
  - BLE advertised name = "XIAO-IMU-" + SENSOR_ROLE (e.g. "XIAO-IMU-RIGHT_LEG"),
    so the browser's device picker shows which physical board is which when
    the user connects each of the 3 roles.
  - A read-only "Role" characteristic also reports SENSOR_ROLE, so the app
    can programmatically confirm it connected to the board it intended to
    (robust against picking the wrong entry in the pairing dialog).

  ARCHITECTURE (per board - identical to the single-sensor design)
  --------------------------------------------------------------------------
  Sampling      : 200 Hz software timer (Ticker) sets a flag; the actual
                  I2C read happens in loop(), never in timer/ISR context.
  Buffering     : RAM ring buffer (1000 samples, ~5s headroom) absorbs
                  transient BLE stalls without dropping data.
  Packetization : 10 samples/packet (~137 bytes) -> 20 notifications/sec
                  instead of 200, the main battery-life lever.
  Reliability   : Sliding-window ACK/resend ARQ. The browser writes back
                  the highest CONTIGUOUS packet sequence it has received;
                  the firmware only retires ring-buffer data once acked,
                  and resends the whole outstanding window in flight if
                  the oldest packet isn't acked within RESEND_TIMEOUT_MS.
                  (notify()==true only means "queued", not "delivered" -
                  this is what actually prevents loss.)
  Power saving  : MPU6050 sleeps and the sample timer detaches whenever
                  not recording or on disconnect. Fast BLE connection
                  interval (7.5-15ms) is explicitly requested on connect.
  Time sync     : Browser writes Date.now() (UTC epoch ms) once per board,
                  ideally with the same captured instant fanned out to all
                  3 boards in quick succession (the app does this) so the
                  three limbs' clocks stay tightly aligned. IST = UTC+5:30
                  is computed on-device and echoed back for confirmation.

  REQUIRED LIBRARY: NimBLE-Arduino (h2zero). Targets v2.x API
  (NimBLEConnInfo& in callbacks) - for v1.x, drop that parameter.
  ============================================================================
*/

#include <NimBLEDevice.h>
#include <Wire.h>
#include <Ticker.h>

// ------------------------- Configuration -----------------------------
#define SAMPLE_RATE_HZ        200
#define SAMPLE_INTERVAL_MS    (1000 / SAMPLE_RATE_HZ)   // 5 ms
#define BATCH_SIZE             10        // samples per BLE packet -> 20 pkt/s
#define RING_BUFFER_SAMPLES    6000      // ~30 s last-resort buffering headroom.
                                          // Bumped up from 5s: field testing showed
                                          // BLE can go fully silent for 60-90s at a
                                          // time when the phone's screen locks or the
                                          // browser tab backgrounds - 30s of on-device
                                          // cushion survives most of that. Memory cost:
                                          // 6000 * 16 bytes = ~94KB SRAM; watch the
                                          // free-heap Serial print at boot if you raise
                                          // this further.
#define MPU_ADDR                0x68

#define WINDOW_SIZE              8       // max packets in flight, unacked (~400ms)
#define RESEND_TIMEOUT_MS        150     // resend outstanding window if not acked in time
#define RESEND_TIMEOUT_MAX_MS   5000     // backoff ceiling during a sustained outage
#define RESEND_BACKOFF_SHIFT_MAX  6      // cap doubling so it can't overflow

// BLE UUIDs - same across all 3 boards; fine, since each board is a
// separate peripheral/connection (no collision).
#define SERVICE_UUID          "6e400001-b5a3-f393-e0a9-e50e24dcca9e"
#define TIME_SYNC_CHAR_UUID   "6e400002-b5a3-f393-e0a9-e50e24dcca9e" // write: 8B epoch ms (UTC, LE)
#define CONTROL_CHAR_UUID     "6e400003-b5a3-f393-e0a9-e50e24dcca9e" // write: 0x01 start / 0x00 stop
#define IMU_DATA_CHAR_UUID    "6e400004-b5a3-f393-e0a9-e50e24dcca9e" // notify: batched IMU packets
#define ACK_CHAR_UUID          "6e400005-b5a3-f393-e0a9-e50e24dcca9e" // write (no rsp): 2B highest contiguous seq received (LE)
#define ROLE_CHAR_UUID          "6e400006-b5a3-f393-e0a9-e50e24dcca9e" // read: ASCII role string, e.g. "RIGHT_LEG"

const uint64_t IST_OFFSET_MS = 19800000ULL; // +5 hours 30 minutes

// ------------------------- Data structures -----------------------------
struct Sample {
  uint32_t elapsedMs;   // ms since time-sync instant
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
uint16_t staleResendStreak  = 0;   // consecutive resend cycles with no ACK progress
uint32_t currentResendTimeoutMs = RESEND_TIMEOUT_MS; // backs off during sustained outage

NimBLEServer*          pServer      = nullptr;
NimBLECharacteristic*  pImuChar     = nullptr;
NimBLECharacteristic*  pTimeChar    = nullptr;
NimBLECharacteristic*  pControlChar = nullptr;
NimBLECharacteristic*  pAckChar     = nullptr;
NimBLECharacteristic*  pRoleChar    = nullptr;

inline int16_t seqDiff(uint16_t a, uint16_t b) { return (int16_t)(a - b); }

// ------------------------- MPU6050 low-level I/O -----------------------------
void mpuWrite(uint8_t reg, uint8_t val) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(reg);
  Wire.write(val);
  Wire.endTransmission();
}

void mpuInit() {
  mpuWrite(0x6B, 0x00); // PWR_MGMT_1: wake up
  delay(10);
  mpuWrite(0x19, 0x04); // SMPLRT_DIV: 1kHz/(1+4) = 200 Hz internal sample rate
  mpuWrite(0x1A, 0x03); // CONFIG: DLPF ~44 Hz bandwidth
  mpuWrite(0x1B, 0x00); // GYRO_CONFIG:  +/-250 dps
  mpuWrite(0x1C, 0x00); // ACCEL_CONFIG: +/-2 g
}

void mpuSleep(bool sleep) {
  mpuWrite(0x6B, sleep ? 0x40 : 0x00);
}

bool mpuReadRaw(int16_t &ax, int16_t &ay, int16_t &az,
                int16_t &gx, int16_t &gy, int16_t &gz) {
  Wire.beginTransmission(MPU_ADDR);
  Wire.write(0x3B);
  if (Wire.endTransmission(false) != 0) return false;
  Wire.requestFrom((uint8_t)MPU_ADDR, (uint8_t)14, (uint8_t)true);
  if (Wire.available() < 14) return false;
  ax = (Wire.read() << 8) | Wire.read();
  ay = (Wire.read() << 8) | Wire.read();
  az = (Wire.read() << 8) | Wire.read();
  Wire.read(); Wire.read(); // discard temperature
  gx = (Wire.read() << 8) | Wire.read();
  gy = (Wire.read() << 8) | Wire.read();
  gz = (Wire.read() << 8) | Wire.read();
  return true;
}

void IRAM_ATTR onSampleTick() { sampleFlag = true; }

void startSampling() {
  mpuSleep(false);
  delay(5);
  rbHead = rbTail = rbCount = 0;
  nextSeq = oldestUnackedSeq = resendCursor = 0;
  resending = false;
  staleResendStreak = 0;
  currentResendTimeoutMs = RESEND_TIMEOUT_MS;
  lastSendMs = millis();
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
    // 12.5-25ms interval (1.25ms units: 10-20). Previously requested the
    // fastest possible 7.5-15ms, but with 3 simultaneous BLE connections
    // (one per limb) sharing the same central radio, that left the
    // central's scheduler little room to fairly interleave all three -
    // this is a likely contributor to one sensor starving another. Still
    // comfortably faster than our ~50ms packet cadence needs.
    srv->updateConnParams(info.getConnHandle(), 10, 20, 0, 400);
    Serial.println("[BLE] Central connected - requested fast conn interval");
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
    staleResendStreak = 0;                          // real progress -> back to fast retry
    currentResendTimeoutMs = RESEND_TIMEOUT_MS;
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
  int16_t ax, ay, az, gx, gy, gz;
  if (!mpuReadRaw(ax, ay, az, gx, gy, gz)) return;
  Sample s;
  s.elapsedMs = millis() - syncElapsedMs;
  s.ax = ax; s.ay = ay; s.az = az;
  s.gx = gx; s.gy = gy; s.gz = gz;
  pushSample(s);
}

// ------------------------- Packet transmission -----------------------------
// 7-byte header + BATCH_SIZE * 13-byte samples (see previous version's
// comment block for the full byte layout - unchanged).
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

  if (inFlight > 0 && (millis() - lastSendMs > currentResendTimeoutMs)) {
    resending = true;
    resendCursor = oldestUnackedSeq;
    // Back off: each unproductive resend cycle doubles the wait (capped),
    // so a genuinely dead link (screen-locked phone, backgrounded tab)
    // doesn't waste airtime/CPU/battery hammering retries every 150ms for
    // minutes on end. The INSTANT any ACK makes real progress, this resets
    // to full-speed 150ms retry (see AckCallbacks) - no lost responsiveness
    // once the link actually comes back.
    if (staleResendStreak < RESEND_BACKOFF_SHIFT_MAX) staleResendStreak++;
    uint32_t backedOff = (uint32_t)RESEND_TIMEOUT_MS << staleResendStreak;
    currentResendTimeoutMs = (backedOff > RESEND_TIMEOUT_MAX_MS) ? RESEND_TIMEOUT_MAX_MS : backedOff;
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

  Wire.begin();
  Wire.setClock(400000);
  mpuInit();
  mpuSleep(true);

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

  // Read-only role identity, so the app can confirm which limb this
  // connection actually is (independent of the advertised name string).
  pRoleChar = pService->createCharacteristic(
      ROLE_CHAR_UUID,
      NIMBLE_PROPERTY::READ);
  pRoleChar->setValue(SENSOR_ROLE);

  pService->start();

  NimBLEAdvertising* pAdv = NimBLEDevice::getAdvertising();
  pAdv->addServiceUUID(SERVICE_UUID);
  pAdv->setName(bleName.c_str());
  pAdv->start();

  Serial.print("[BLE] Advertising as '");
  Serial.print(bleName);
  Serial.println("' - waiting for connection...");
  Serial.printf("[MEM] Free heap at boot: %u bytes (ring buffer uses ~%u bytes)\n",
                 ESP.getFreeHeap(), (unsigned)(RING_BUFFER_SAMPLES * sizeof(Sample)));
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

  // Lightweight periodic diagnostics while recording - handy for
  // confirming buffering/backoff behavior on the Serial monitor during
  // a long-duration test.
  static uint32_t lastDiagMs = 0;
  if (recording && millis() - lastDiagMs > 5000) {
    lastDiagMs = millis();
    Serial.printf("[DIAG] rbCount=%u inFlight=%u resending=%d resendTimeout=%ums freeHeap=%u\n",
                   rbCount, (unsigned)seqDiff(nextSeq, oldestUnackedSeq), resending,
                   currentResendTimeoutMs, ESP.getFreeHeap());
  }
}
