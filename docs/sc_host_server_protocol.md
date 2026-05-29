# StarCraftHost 当前服务端协议文档

本文档描述当前 `sc_host` 服务端实现的 UDP 协议、端口职责、包结构、收包处理规则和发包时机。它是实现文档，不是纯 StarCraft/Brood War LAN 协议规范；公开协议规则见 `docs/starcraft_udp_protocol.md`。

当前服务端目标是模拟一个可配置 LAN 房间，并在需要时代理玩家之间的 UDP 通信。

## 1. 当前模型

### 1.1 玩家与槽位

玩家身份从 `sc_host.ini` 读取；没有配置文件时使用内置 2 人默认值。示例配置：

| 名称 | 角色 | slot | player_id |
| --- | --- | --- | --- |
| `Sun` | MainHost | `0` | `1` |
| `SunX` | SubHost | `1` | `2` |
| `SunY` | Player | `2` | `3` |

服务端自身在 Storm 包头中使用 `WIRE_HOST_ID = 0` 作为 host/系统发送方。

注意：当前 slot 快照只包含配置中的真实玩家，不把 `player_id=0` 作为真实 slot 玩家加入 `SLOTUPDATE/NEWNETPLAYER`。wire 层仍保留 `record_pid=0, is_host=1` 给客户端建立“可见 host”身份。

### 1.2 可见 host 名称

当前服务端会根据加入者构造“对方是可见 host”的效果：

| 加入者 | visible host name | visible host slot | visible host id |
| --- | --- | --- | --- |
| `Sun` | `SunX` | `1` | `2` |
| `SunX` | `Sun` | `0` | `1` |
| 其他玩家 | `Sun` | `0` | `1` |

但 wire 层的 host record 仍使用 `record_pid=0, is_host=1`。

### 1.3 房间级 SYNC tick

当前实现维护房间级 `room_sync_next_send`，初始值为 `100`。

所有服务端主动发出的大厅/开局 `CLS_SYNC` 包，包括 `NOP`、`SLOTUPDATE`、`STARTGAME`、`SEED`，使用房间级全局 seq。也就是说，同一轮 tick 发给多个玩家时，`seq_send` 相同。

每个玩家仍各自维护：

- `last_recv[cls]`：服务端认为该玩家下一个会发来的 seq。
- `history[cls][seq]`：发给该玩家的历史包，用于 resend。
- `pending_sync_packets`：等待下一次 room sync tick 发送给该玩家的业务包。

## 2. 端口职责

### 2.1 UDP 6111: LAN discovery

用途：

- 接收 StarCraft LAN 搜房请求。
- 单播回复房间广告。
- 周期性向广播地址主动广播房间广告。

服务端默认绑定：

```text
0.0.0.0:6111
```

处理规则：

1. 解开 Blizzard UDP wrapper。
2. 按 `LanPacket` 解析。
3. 只处理：
   - `product == PRODUCT_SEXP`
   - `version == 0xC3`
   - `kind == 2`
4. 回复 `RoomAdvertisement(kind=0)`。

周期广播：

- 默认每 `advertise_interval = 2.0s` 广播一次。
- `starting` 或 `started` 后停止广播。

### 2.2 UDP 6112: Storm 主通道

用途：

- 加入房间握手。
- 控制包 `CLS_CONTROL`。
- 大厅异步包 `CLS_ASYNC`。
- 大厅/开局同步包 `CLS_SYNC`。
- 服务端可靠 UDP resend/verify。

服务端默认绑定：

```text
0.0.0.0:6112
```

### 2.3 动态 UDP relay 端口

用途：

- 代理客户端之间的 peer UDP 通信。
- 在 `CMD_PLAYER` 真实玩家记录里，把对方地址写成服务端动态 relay 地址。

每个方向会创建一条 route：

```text
(viewer_real_addr, subject_real_addr) -> advertised_server_addr
```

例子：

```text
Sun 看到 SunX 地址为 Server:53271
SunX 看到 Sun 地址为 Server:53273
```

转发规则：

```text
Sun -> Server:53271
Server 使用反向 route 的 socket Server:53273 -> SunX
```

这样 SunX 看到的来源地址就是它记录中的 Sun 地址。

## 3. 通用封包

### 3.1 UDP wrapper

所有 LAN/Storm 包外层都有统一 wrapper：

| Offset | Type | Meaning |
| --- | --- | --- |
| `0x00` | `uint16` | checksum |
| `0x02` | `uint16` | packet length |
| `0x04` | bytes | body |

实现位置：

- `checksum.wrap_body()`
- `checksum.unwrap_body()`

### 3.2 LAN body

```text
uint32 kind
char[4] product
uint32 version
uint32 state
bytes payload
```

当前常量：

| Field | Value |
| --- | --- |
| `PRODUCT_SEXP` | `b"PXES"` |
| `VERSION_CODE` | `0xC3` |

### 3.3 Storm body

```text
uint16 seq_send
uint16 seq_recv
uint8  cls
uint8  command
uint8  player_id
uint8  status
bytes  payload
```

字段意义：

| Field | Meaning |
| --- | --- |
| `seq_send` | 本包在当前 `CLS` 可靠流里的发送序号 |
| `seq_recv` | 本端认为对方下一包应该使用的序号 |
| `cls` | Storm class |
| `command` | `CLS_CONTROL` 时的 control command；其他 class 通常为 `0` |
| `player_id` | Storm sender/player id |
| `status` | reliable 状态 |
| `payload` | command 或 SCGP payload |

### 3.4 CLS

| Value | Name | Current use |
| --- | --- | --- |
| `0` | `CLS_CONTROL` | 加入、PING/PONG、PLAYER、GAMEDATA、GAMESTATE、QUIT |
| `1` | `CLS_ASYNC` | ROOMDATA、UNKNOWNREQUEST、MAP、PLAYERJOIN、LOBBYCHAT |
| `2` | `CLS_SYNC` | NOP、JOINEDGAME、MAPPERCENT、SLOTUPDATE、STARTGAME、SEED、游戏内同步 |

### 3.5 STATUS

| Value | Name | Current handling |
| --- | --- | --- |
| `0` | `STATUS_NORMAL` | 正常包；更新 `last_recv[cls]` |
| `1` | `STATUS_VERIFY` | 校验状态；`CONTROL VERIFY` 可触发 ROOMDATA/UNKNOWNREQUEST |
| `2` | `STATUS_RESEND_REQUEST` | 请求重发 |
| `3` | `STATUS_RESEND_RESPONSE` | 当前未特别处理 |

## 4. LAN discovery

### 4.1 收到搜房请求

客户端请求：

```text
LAN kind=2, product=SEXP, version=0xC3
```

服务端处理：

```text
if product/version/kind 不匹配:
    ignore
else:
    send RoomAdvertisement
```

### 4.2 房间广告包

`RoomAdvertisement` payload：

```text
cstring host_name
cstring stat_string
uint32  game_type
uint32  current_players
uint32  max_players
```

当前填法：

| Field | Value |
| --- | --- |
| `host_name` | `self.host_name`，默认 `SunX` |
| `stat_string` | `make_stat_string(host_name, room_name)` |
| `game_type` | `12` |
| `current_players` | `max(1, min(real_joined_players, max_players))` |
| `max_players` | 配置的玩家数量 |
| `state` | 房间可加入为 `0`，满员/开局中为 `0x0E` |

`state` 规则：

```text
if real_player_count < max_players and not starting and not started:
    state = 0
else:
    state = 0x0E
```

## 5. 可靠层

### 5.1 收包更新

收到 `STATUS_NORMAL` 包时：

```text
last_recv[packet.cls] = packet.seq_send + 1
```

### 5.2 发包序号

`CLS_CONTROL`、`CLS_ASYNC`：

- 每个 session 独立 `next_send[cls]`。
- 发包后自增。

`CLS_SYNC`：

- 服务端主动大厅/开局包使用房间级 `room_sync_next_send`。
- 同一 tick 发给多个玩家时使用相同 `seq_send`。
- 发完一轮 tick 后 `room_sync_next_send += 1`。

### 5.3 历史缓存

每个 session、每个 `CLS` 保存最近 256 个 normal 发包历史：

```text
history[cls][seq_send] = wire_packet
```

用于处理 resend。

### 5.4 VERIFY

发送 verify 时：

```text
seq = last_recv[cls]
StormPacket(seq, seq, cls, command=0, player_id=host_id, status=VERIFY)
```

## 6. Resend 处理

### 6.1 通用入口

收到 `STATUS_RESEND_REQUEST`：

```text
if session 不存在:
    ignore
build ResendRequestContext
if CLS_SYNC 且 payload 指向其他 player:
    forward to peer context
elif requested_seq == next_send:
    send/throttle next sync tick
elif history 命中:
    resend history packet
else:
    send VERIFY
```

### 6.2 requested seq 选择

`CLS_SYNC`：

- `chosen_requested_seq = header.seq_recv`
- payload 第 1 字节如果存在，作为 `payload_context_player_id`
- 不把 `payload[0:2]` 当 16-bit seq 使用

非 `CLS_SYNC`：

- 如果 `payload_len >= 2`，当前仍按 `payload[0:2]` 小端解释为 requested seq。
- 否则使用 `header.seq_recv`。

### 6.3 SYNC peer context

如果 `CLS_SYNC RESEND_REQUEST` 的 payload 第 1 字节是另一个真实玩家 id，例如：

```text
Sun -> Server: payload=02
```

服务端认为这是 player 2 的 peer context 请求：

```text
target = session_for_player_id(2)
if target 存在:
    转发原始 wire 到 target，使用反向 relay socket
else:
    send VERIFY
```

### 6.4 next_send resend

如果客户端请求的 seq 等于当前 `room_sync_next_send`：

- 认为客户端在等下一轮 sync tick。
- 最多按 `RESEND_NEXT_SEND_SYNC_INTERVAL = 0.25s` 发送下一轮 room sync tick。
- 过快请求只记录 throttle 日志，不刷 NOP。

## 7. Control 包

### 7.1 Control command 表

| Command | Name | Direction |
| --- | --- | --- |
| `0x01` | `REQUESTJOIN` | client -> server |
| `0x02` | `REQUESTJOINOK` | server -> client |
| `0x03` | `REQUESTJOIN2` | client -> server |
| `0x04` | `PING` | both |
| `0x05` | `PONG` | both |
| `0x06` | `PLAYER` | server -> client |
| `0x07` | `ENTER` | client -> server |
| `0x08` | `GAMEDATA` | server -> client |
| `0x09` | `GAMETYPE` | server -> client |
| `0x0A` | `JOINFAIL` | server -> client |
| `0x0B` | `QUIT` | both |
| `0x0E` | `GAMESTATE` | server -> client |
| `0x0F` | `STATSCODE` | server -> client |

### 7.2 REQUESTJOIN

客户端：

```text
CLS_CONTROL CMD_REQUESTJOIN player_id=0xff
```

服务端处理：

1. 如果已有同地址 pending session 且还没 ready，可重置。
2. 如果 session 数已经达到配置的玩家数量，发送 `JOINFAIL`。
3. 创建 session。
4. 初始化该 session 的 `CLS_SYNC next_send/last_recv` 为当前 `room_sync_next_send`。
5. 发送：

```text
CLS_CONTROL CMD_REQUESTJOINOK payload=<uint32 1>
```

### 7.3 REQUESTJOIN2

当前只记录日志，不主动发包。

### 7.4 ENTER

客户端 payload：

```text
cstring player_name
```

服务端处理：

1. 解析玩家名。
2. 名字必须匹配 `sc_host.ini` 中的 `[players]` / `[player.<name>]` 配置。
3. 根据配置分配 slot、player id、team、race。
4. 如果 slot 已被占用，发送 `JOINFAIL`。
5. 标记：
   - `session.joined = True`
   - `session.slot_index = configured slot`
   - `session.reliable.player_id = configured id`
   - `session.team = configured team`
   - `session.race = configured race`
6. 先向已进入 lobby 的老玩家发送新玩家 `CMD_PLAYER` 记录。
7. 给当前加入者发送 join handshake。

### 7.5 Join handshake 发包顺序

进入 `ENTER` 后，服务端给加入者发送：

```text
GAMEDATA
PLAYER(record_pid=0, is_host=1, visible host)
[PLAYER(real visible peer, is_host=0, relay addr)]      # 如果 visible host 已存在
[PLAYER(other existing peer, is_host=0, relay addr)]    # 如果还有其他已加入玩家
STATSCODE
GAMETYPE
SYNC NOP bootstrap
PING
```

#### GAMEDATA payload

```text
uint32 assigned_player_id
uint32 max_players
uint32 command2_packet_count
uint32 unknown
uint32 game_uptime_seconds
cstring visible_host_name
cstring stat_string
cstring password
```

当前填法：

| Field | Value |
| --- | --- |
| `assigned_player_id` | 配置中的当前玩家 id |
| `max_players` | 配置的玩家数量 |
| `command2_packet_count` | `room_sync_next_send` |
| `unknown` | `0x06` |
| `game_uptime_seconds` | `time.monotonic() - room_created_at` |
| `visible_host_name` | 对加入者可见的 host 名 |
| `stat_string` | `make_stat_string(visible_host_name, room_name)` |
| `password` | 空 |

#### PLAYER payload

```text
uint32 size
uint32 player_id
uint32 is_host
uint32 peer_flag
uint32 command2_packet_count
sockaddr_in sockaddr
cstring name
cstring stat_string
```

当前填法：

| Record | player_id | is_host | peer_flag | sockaddr | name |
| --- | --- | --- | --- | --- | --- |
| wire host | `0` | `1` | `0` | `0.0.0.0:0` | visible host name |
| real peer | subject pid | `0` | `4` | relay addr | subject name |

`command2_packet_count` 使用 `room_sync_next_send`。

#### STATSCODE

```text
uint32 stat_code = 0
```

#### GAMETYPE

当前是固定 32 字节抓包常量：

```text
0f 00 01 00 01 00 00 00
01 01 01 02 02 00 01 01
00 01 00 00 00 00 00 32
00 00 00 00 00 00 00 00
```

#### SYNC NOP bootstrap

`JOIN_BOOTSTRAP_SYNC_NOP_COUNT = 2`。

第一个玩家加入时：

```text
send NOP seq=room_sync_next_send
room_sync_next_send += 1
send NOP seq=room_sync_next_send
room_sync_next_send += 1
```

后续玩家加入时：

```text
send NOP seq=room_sync_next_send - 2
send NOP seq=room_sync_next_send - 1
room_sync_next_send 不变
```

目的是让新玩家补到当前房间 sync 位置。

#### PING

发送 `CMD_PING` 后记录：

```text
join_ping_ack_seq = sent_ping.seq_send + 1
```

后续要等客户端 `PONG` 和 `CONTROL VERIFY` 后才发送 `ROOMDATA/UNKNOWNREQUEST`。

### 7.6 PING/PONG

收到客户端 `CMD_PING`：

```text
send CMD_PONG
```

收到客户端 `CMD_PONG` 且 session 已 joined：

```text
send CONTROL VERIFY
note join ping ack
```

### 7.7 CONTROL VERIFY 后的大厅快照门控

服务端不会在 `ENTER` 后立即发 `ROOMDATA`。它等待：

1. 已发送 join PING。
2. 客户端 PONG 的 `seq_recv` 到达 `join_ping_ack_seq`。
3. 客户端再发 `CLS_CONTROL STATUS_VERIFY`。

满足后发送：

```text
CLS_ASYNC ROOMDATA
CLS_ASYNC UNKNOWNREQUEST
```

### 7.8 QUIT

收到 `CMD_QUIT`：

1. 向其他 joined 玩家转发 `CMD_QUIT`，`player_id` 为离开玩家 id。
2. 移除 session。
3. 关闭与该玩家相关的 relay。
4. 给剩余 ready 玩家排队发送 slot 快照，使离开槽位变回 open/empty。

## 8. ASYNC / SCGP 包

### 8.1 SCGP 表

| ID | Name | Current use |
| --- | --- | --- |
| `0x49` | `PLAYERJOIN` | 玩家进入 lobby 后广播 |
| `0x4A` | `ROOMDATA` | 初始地图/房间 slot 数据 |
| `0x4F` | `MAP` | 地图信息、地图完成、开局 map complete |
| `0x50` | `UNKNOWNREQUEST` | ROOMDATA 后提示客户端继续流程 |
| `0x4C` | `LOBBYCHAT` | 大厅聊天转发 |

### 8.2 ROOMDATA

当前 `ROOMDATA` 长度 63 bytes，以 `0x4A` 开头，由 `sc_host.ini [roomdata]` 组装：

```text
uint8  id = 0x4A
uint16 tileset
uint16 width
uint16 height
uint8  ownr[12]
uint8  side[12]
uint8  ownr_default[12]
uint8  forc[8]
uint8  forc_flags[4]
uint8  race[8]
```

发送时机：

```text
join PING/PONG + CONTROL VERIFY gate satisfied
```

发送方向：

```text
server -> joining client
```

### 8.3 UNKNOWNREQUEST

payload：

```text
50
```

发送时机：

```text
紧跟 ROOMDATA 之后
```

### 8.4 JOINEDGAME 后的 PLAYERJOIN/MAP

收到客户端 `CLS_SYNC SCGP_JOINEDGAME`：

1. 标记 `session.lobby_ready = True`。
2. 如果这是首次 ready：
   - 向所有 joined 玩家发送 `CLS_ASYNC PLAYERJOIN(joined_player_id)`。
3. 如果该玩家还没发过 map bootstrap：
   - 给该玩家发送 `CLS_ASYNC MAP(kind=0x0001)`。
4. 检查是否可以 auto start。

### 8.5 MAP(kind=0x0001)

服务端发送的 map info：

```text
uint8  id = 0x4F
uint16 length
uint16 kind = 0x0001
uint32 map_size      # from sc_host.ini [map] size
uint32 map_checksum  # from sc_host.ini [map] checksum
cstring map_name     # from sc_host.ini [map] name
```

### 8.6 MAP(kind=0x0000)

客户端对 map info 的响应。当前解析：

```text
uint8  id = 0x4F
uint16 length
uint16 kind = 0x0000
uint16 request_value
uint32 file_position
```

服务端处理：

```text
send ASYNC VERIFY
if file_position >= configured map_size:
    map_percent = 100
    queue MAPPERCENT/SLOTUPDATE/NEWNETPLAYER to this player
    queue same snapshot to ready old players
else:
    每 1s 重新发送 MAP(kind=0x0001)
    日志提示 map block sending not implemented
```

当前不实现地图 block 传输；依赖客户端已有地图。

### 8.7 LOBBYCHAT

收到：

```text
CLS_ASYNC SCGP_LOBBYCHAT cstring message
```

处理：

```text
转发给其他 joined 玩家
```

### 8.8 其他 ASYNC

未知或未特别处理的 ASYNC payload：

```text
转发给其他 joined 玩家
```

## 9. SYNC / SCGP 包

### 9.1 SYNC tick

非开局、未开始时，服务端每 `LOBBY_SYNC_NOP_INTERVAL = 0.25s` 推进一次房间 sync tick：

```text
if session.pending_sync_packets:
    发 pending 业务包
else:
    发 SCGP_NOP
```

同一轮 tick 对所有 joined 玩家使用相同 `seq_send`。

开局过渡阶段，tick 间隔改为：

```text
START_TRANSITION_SYNC_INTERVAL = 0.05s
```

### 9.2 NOP

payload：

```text
05
```

用途：

- 没有业务 sync 包时推进 `CLS_SYNC`。
- 让客户端确认当前 sync seq。

### 9.3 MAPPERCENT

客户端发：

```text
3D percent
```

服务端处理：

```text
session.map_percent = percent
mark lobby activity
maybe schedule auto start
```

服务端发：

```text
3D 64
```

通常作为 slot 快照第一段，与 `SLOTUPDATE/NEWNETPLAYER` 合包。

### 9.4 SLOTUPDATE

单条结构：

```text
uint8 id = 0x3E
uint8 slot
uint8 player
uint8 state
uint8 race
uint8 team
```

当前状态：

| Meaning | player | state |
| --- | --- | --- |
| active player | player id | `2` |
| open empty slot | `0xff` | `6` |
| disabled/unused slot | `0xff` | `0` |

当前 slot 快照顺序按 `max_slots - 1` 到 `0` 逆序生成，默认至少 8 个 slot：

```text
slot 7 disabled
slot 6 disabled
slot 5 disabled
slot 4 disabled
slot 3 disabled or configured player/open
slot 2 configured player/open
slot 1 configured player/open
slot 0 configured player/open
NEWNETPLAYER...
```

配置中的 slot：

```text
player = configured player_id if that player is lobby_ready else 0xff
state  = 2 if active else 6
race   = session.race if active else configured race
team   = session.team if active else configured team
```

未配置的 slot：

```text
player = 0xff
state  = 0
race   = visual filler pattern 0/1/2
team   = 0
```

### 9.5 NEWNETPLAYER

结构：

```text
uint8  id = 0x3F
uint8  player_id
uint16 unknown0 = 0
uint16 unknown1 = 1
uint16 unknown2 = 5
```

当前生成顺序：

1. 按 active slot 从高到低加入对应 `player_id`。
2. 默认不加入 virtual host `0`。

### 9.6 CHANGERACE

收到：

```text
41 unknown race
```

处理：

```text
session.race = payload[2]
mark lobby activity
queue slot snapshot to all joined players
```

### 9.7 STARTGAME

客户端发来的 `STARTGAME` 当前忽略。

服务端发送时机见“开局流程”。

### 9.8 SEED

结构：

```text
uint8  id = 0x48
uint32 seed
bytes  extra = 08 08 08 08 08 08 08 08
```

当前 seed：

```text
seed = int(time.time())
```

服务端只生成一次 payload，并用同一房间 sync seq 发给所有 joined 玩家。

## 10. 加入房间完整流程

### 10.1 第一个玩家加入

```text
Client -> Server  REQUESTJOIN
Server -> Client  REQUESTJOINOK
Client -> Server  REQUESTJOIN2
Client -> Server  ENTER(name)
Server -> Client  GAMEDATA
Server -> Client  PLAYER(record_pid=0, is_host=1)
Server -> Client  STATSCODE
Server -> Client  GAMETYPE
Server -> Client  SYNC NOP x2
Server -> Client  PING
Client -> Server  PONG
Server -> Client  CONTROL VERIFY
Client -> Server  PING
Server -> Client  PONG
Client -> Server  CONTROL VERIFY
Server -> Client  ROOMDATA
Server -> Client  UNKNOWNREQUEST
Client -> Server  JOINEDGAME
Server -> Client  PLAYERJOIN(self)
Server -> Client  MAP(kind=0x0001)
Client -> Server  MAP(kind=0x0000, file_position >= map_size)
Server -> Client  ASYNC VERIFY
Server -> Client  MAPPERCENT(100)+SLOTUPDATE+NEWNETPLAYER
```

### 10.2 第二个玩家加入

假设 `Sun` 已经 ready，`SunX` 加入：

```text
SunX -> Server  REQUESTJOIN
Server -> SunX  REQUESTJOINOK
SunX -> Server  REQUESTJOIN2
SunX -> Server  ENTER("SunX")
Server -> Sun   PLAYER(record_pid=2, addr=relay_to_SunX)
Server -> SunX  GAMEDATA(assigned=2, command2=room_sync_next_send)
Server -> SunX  PLAYER(record_pid=0, is_host=1, visible host Sun)
Server -> SunX  PLAYER(record_pid=1, addr=relay_to_Sun)
Server -> SunX  STATSCODE
Server -> SunX  GAMETYPE
Server -> SunX  backfill SYNC NOP x2
Server -> SunX  PING
SunX -> Server  PONG / PING / VERIFY
Server -> SunX  ROOMDATA
Server -> SunX  UNKNOWNREQUEST
SunX -> Server  JOINEDGAME
Server -> all   PLAYERJOIN(2)
Server -> SunX  MAP(kind=0x0001)
SunX -> Server  MAP(kind=0x0000, complete)
Server queues   slot snapshot to SunX and ready old players
Next sync tick:
Server -> Sun   MAPPERCENT+SLOTUPDATE+NEWNETPLAYER seq=X
Server -> SunX  MAPPERCENT+SLOTUPDATE+NEWNETPLAYER seq=X
```

### 10.3 老玩家和新玩家的 peer 通信

服务端通过 `CMD_PLAYER` 告诉两边对方的 relay 地址。

随后客户端之间可能出现：

```text
Sun  -> relay(SunX)  PING / PONG / VERIFY / JOINEDGAME / MAPPERCENT / SYNC
relay -> SunX        原始 wire 转发
SunX -> relay(Sun)   PING / PONG / VERIFY / JOINEDGAME / MAPPERCENT / SYNC
relay -> Sun         原始 wire 转发
```

relay 不解析和改写 Storm 包，只校验来源地址并转发原始 bytes。

## 11. 开局流程

### 11.1 ready 条件

服务端只有满足以下条件才会 auto start：

```text
joined players == configured max_players
every configured slot has a joined session
all lobby_ready
all map_percent >= 100
stable for start_stability_delay
```

默认：

```text
start_stability_delay = 1.0s
auto_start_delay = 3.0s
game_state_delay = 0.35s
seed_delay = 5.75s
```

### 11.2 发包顺序

```text
Server -> all  GAMESTATE              # CLS_CONTROL CMD_GAMESTATE
wait game_state_delay
Server -> all  STARTGAME              # CLS_SYNC, same room seq
start 50ms NOP transition loop
wait seed_delay
stop transition loop
Server -> all  SEED                   # CLS_SYNC, same room seq, same seed
Server -> all  MAP(kind=0x0003)       # CLS_ASYNC map complete
started = True
```

### 11.3 开局过渡 NOP

`STARTGAME` 后到 `SEED` 前：

```text
every 0.05s:
    send room sync tick
```

每轮 tick 仍使用房间级全局 seq。

## 12. 游戏中转发

当 `started == True` 后：

收到客户端 `CLS_SYNC` 且不是服务端特别处理的大厅包：

```text
trace if enabled
send payload to other joined players
```

当前 `_send_to_others()` 使用接收方 session 的 reliable send，`player_id` 设置为原发送者的 player id。这个路径与大厅/开局的房间级 room sync tick 不完全相同，需要后续结合游戏内抓包继续校准。

## 13. 断线/离开

收到 `CMD_QUIT`：

```text
for other joined players:
    send CMD_QUIT with leaving player's player_id
remove session
close all relay routes involving this session
queue SLOTUPDATE snapshot to remaining ready players
```

离开后的槽位：

```text
player = 0xff
state = 6
race = 6
```

## 14. 当前配置和常量

默认读取根目录 `sc_host.ini`：

```ini
[room]
name = King of the Hill
main_host = Sun
sub_host = SunX
advertise_host = SunX

[map]
name = (4)King of the Hill.scm
size = 0x00011DE9
checksum = 0xCBB55E68

[roomdata]
tileset = 4
width = 128
height = 128
ownr = 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0
side = 6, 6, 6, 0, 0, 2, 1, 0, 0, 0, 0, 0
ownr_default = 6, 6, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0
forc = 1, 1, 1, 0, 0, 0, 0, 0
forc_flags = 1, 0, 0, 0
race = 1, 1, 1, 0, 0, 0, 0, 0

[players]
names = Sun, SunX, SunY

[player.Sun]
id = 1
slot = 0
team = 1
race = 6
```

命令行可覆盖 `room/map/main_host/sub_host/advertise_host`，但玩家列表仍来自 INI。

| Name | Value |
| --- | --- |
| `DEFAULT_MAIN_HOST_NAME` | `Sun` |
| `DEFAULT_SUB_HOST_NAME` | `SunX` |
| `WIRE_HOST_ID` | `0` |
| `MAIN_HOST_PLAYER_ID` | `1` |
| `SUB_HOST_PLAYER_ID` | `2` |
| `UNKNOWN_PLAYER_ID` | `0xff` |
| `LOBBY_SYNC_NOP_INTERVAL` | `0.25s` |
| `START_TRANSITION_SYNC_INTERVAL` | `0.05s` |
| `RESEND_NEXT_SEND_SYNC_INTERVAL` | `0.25s` |
| `JOIN_BOOTSTRAP_SYNC_NOP_COUNT` | `2` |
| `DEFAULT_CONFIG_PATH` | `sc_host.ini` |
| `DEFAULT_MAP_FILE_NAME/SIZE/CHECKSUM` | 无 INI 时的内置 fallback |

## 15. 已知限制和待校准点

1. 玩家身份、slot、地图 `ROOMDATA` 已配置化，但不同地图的 `OWNR/SIDE/FORC/RACE` 仍需按抓包或地图 CHK 继续校准。
2. 当前不实现地图 block 传输，只处理客户端已有完整地图的情况。
3. 当前 slot 快照不包含 `pid=0` 作为真实 host 玩家；真实多人主机抓包会包含 `pid=0`。
4. `GAMETYPE` 仍是固定抓包常量。
5. `CMD_PLAYER.peer_flag` 对新玩家/老玩家方向的真实含义仍未完全确认。
6. `STATUS_RESEND_RESPONSE/CALLBACK` 未完整实现。
7. 游戏开始后的 `CLS_SYNC` 转发路径还需要继续结合游戏内抓包校准。
8. `SEED` 使用 `int(time.time())`，不是可配置或可复现 seed。
