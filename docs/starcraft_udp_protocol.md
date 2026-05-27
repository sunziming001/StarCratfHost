# StarCraft/Brood War UDP LAN Protocol Notes

本文整理 StarCraft/Brood War 局域网对局相关的 UDP、Storm、SCGP 协议规则，作为后续实现和调试依据。

范围仅包括 LAN 房间发现、`PKT_STORM`、SCGP 大厅/开局/同步包、statstring 和可靠 UDP 语义。不覆盖 Battle.net TCP/BNCS 登录、账号、CD-Key、ladder、完整 MPQ/CHK 地图格式，也不描述任何具体项目的 host 模型。

## 资料来源和可信度

主要资料：

- [BNETDocs: PKT_STORM](https://bnetdocs.org/packet/482/pkt-storm)
- [Valhalla Legends: StarCraft UDP / SCGP specification](https://vl.bnetdocs.org/index.php?topic=17702.0)
- [BNETDocs: Game Statstrings](https://bnetdocs.org/document/13/game-statstrings)
- [BNETDocs Redux: SCGP_SEED](https://redux.bnetdocs.org/?op=packet&pid=502)

可信度标记：

- **公开资料确认**：公开逆向资料明确列出的字段或语义。
- **从资料推断**：公开资料没有完整 wire 例子，但多个字段说明可以互相印证。
- **抓包/实现待验证**：来自抓包常量、现有实现或经验推测，需要继续用抓包验证。

公开资料本身是第三方逆向结果，并非 Blizzard 官方协议文档。存在字段命名不一致、长度含糊、不同 StarCraft/Brood War 版本行为不同的风险。

## 基础约定

- 多字节整数通常使用 little-endian。
- 字符串通常是 Latin-1/ANSI 编码并以 `NUL` 结尾。
- StarCraft 经典局域网协议主要使用 UDP：
  - UDP `6111`：LAN 房间发现和房间广告。
  - UDP `6112`：Storm/SCGP 对局流量。
- `STAR` 表示 StarCraft，`SEXP` 表示 Brood War。LAN wire 中常见 product 字节序可能表现为 `PXES`，即 little-endian 视角下的 `SEXP`。
- Brood War 公开资料和抓包中常见 version code 为 `0xC3`。

## 通用 UDP 包封装

### Packet wrapper

公开资料和实现经验表明，LAN/Storm UDP payload 外层有统一 wrapper：

| Offset | Type | Meaning |
| --- | --- | --- |
| `0x00` | `uint16` | Blizzard UDP checksum |
| `0x02` | `uint16` | packet total length, including checksum and length fields |
| `0x04` | bytes | protocol body |

实现注意：

- 接收时应验证 `length == actual packet length`。
- checksum 验证失败时应丢弃或记录为 malformed packet。
- 发送时先写 length，再按 Blizzard UDP checksum 算法计算 checksum。
- checksum 算法公开资料较少，建议以本地抓包回放和互通测试验证。

## LAN Discovery

LAN discovery 用于客户端在局域网列表中发现可加入房间。该部分公开资料较少，以下 wire 结构主要来自抓包/实现待验证。

### LAN packet body

| Offset | Type | Meaning |
| --- | --- | --- |
| `0x00` | `uint32` | kind |
| `0x04` | `char[4]` | product, e.g. little-endian `SEXP` as `PXES` |
| `0x08` | `uint32` | version code, Brood War 常见 `0xC3` |
| `0x0C` | `uint32` | advertised state |
| `0x10` | bytes | kind-specific payload |

### Discovery request

抓包/实现待验证：

- 客户端向 UDP `6111` 发送查询。
- `kind=2` 可视为房间搜索请求。
- 服务端/房主收到合法 product/version 查询后，向请求来源返回房间广告。

### Room advertisement

抓包/实现待验证的 payload：

| Field | Type | Meaning |
| --- | --- | --- |
| `host_name` | `cstring` | 房间列表中显示的创建者/房主名 |
| `stat_string` | `cstring` | 游戏属性和地图信息 |
| `game_type` | `uint32` | 房间类型，常见值需结合 statstring 判断 |
| `current_players` | `uint32` | 当前玩家数 |
| `max_players` | `uint32` | 最大玩家数 |

房间状态通常由 LAN packet header 的 `state` 表示。实践中：

- `state=0` 通常表示可加入。
- 非零状态可能表示进行中、关闭或不可加入；具体 bit 语义需要抓包确认。

## PKT_STORM

`PKT_STORM` 是 UDP `6112` 上承载 SCGP/游戏流量的外层协议。BNETDocs 将它描述为 P2P StarCraft packet。字段如下。

### Storm packet body

| Offset | Type | Name | Meaning |
| --- | --- | --- | --- |
| `0x00` | `uint16` | `Seq1` / `seq_send` | 本端发送计数 |
| `0x02` | `uint16` | `Seq2` / `seq_recv` | 本端已接收/期待对端计数 |
| `0x04` | `uint8` | `CLS` | packet class |
| `0x05` | `uint8` | `Command` | control command 或 class-specific command |
| `0x06` | `uint8` | `PlayerID` | 与该包相关的 player id |
| `0x07` | `uint8` | `Status` / `Resend` | reliable status |
| `0x08` | bytes | `Payload` | class/command-specific payload |

实现注意：

- `Seq1` 和 `Seq2` 是 reliable layer 的核心字段。
- `Status=0` 的正常包通常应递增发送序号。
- `Status!=0` 的控制性 reliable 包不一定应递增正常发送序号，需以抓包确认。
- `PlayerID=0xFF` 在资料中可作为 unknown/no player 使用；实际对局中也常见具体玩家 ID。

## Storm CLS 分类

| CLS | Constant | Name | Meaning |
| --- | --- | --- | --- |
| `0` | `CLS_CONTROL` | Control / internal | 加入、玩家信息、ping、game metadata 等控制包 |
| `1` | `CLS_ASYNC` | Async | 大厅聊天、地图请求、部分非 turn 同步事件 |
| `2` | `CLS_SYNC` | Sync | 需要按 turn/序号严格同步的游戏事件 |

公开资料把 `CLS=0` 的 command 表称为 internal packets，把 `CLS=1/2` 的 payload 视为 SCGP game packets。

## Reliable status

| Value | Constant | Name | Meaning |
| --- | --- | --- | --- |
| `0x00` | `STATUS_NORMAL` | Normal | 普通数据包 |
| `0x01` | `STATUS_VERIFY` | Verify | 确认/校验当前可靠状态 |
| `0x02` | `STATUS_RESEND_REQUEST` | Resend request | 请求重发某个计数/包 |
| `0x03` | `STATUS_RESEND_RESPONSE` | Callback / resend response | 公开资料有歧义；可能和 callback/重发响应有关 |

### Resend/verify 语义

公开资料确认：

- `Status=0x02` 表示请求重发。
- `Status=0x01` 表示 verify。
- Storm header 中 `Seq1/Seq2` 和 reliable status 共同决定可靠层状态。

从资料推断：

- resend 请求的目标 count 不应盲目从 payload 前两个字节读取。
- `Seq2` 更可能表示“请求/确认的接收计数”。
- 对 `CLS=2`，资料提到某些 resend/callback 情况下 header 后可能追加一个 byte，用于标识另一个 player id；该 byte 不等同于 16-bit sequence。

实现建议：

- 记录 resend request 时同时打印 `Seq1`、`Seq2`、`CLS`、`Command`、`PlayerID`、`Status`、`payload_len`、`payload_head`。
- 重发历史应按 `CLS` 和 `Seq1` 缓存正常发送包。
- 处理 resend 时优先按公开资料语义使用 Storm header 字段；payload 解析应按 class/command/status 区分。
- 不应把“payload 长度大于等于 2”直接视为“payload 前两个字节是 requested seq”，除非抓包确认。

## CLS=0 Control commands

Valhalla SCGP spec 将以下 command 描述为 control/internal packets。字段名和长度在公开资料中有些不完整，下面按实现常用语义整理。

| Command | Constant | Name | Direction | Payload summary |
| --- | --- | --- | --- | --- |
| `0x01` | `CMD_REQUESTJOIN` | `REQUESTJOIN` | client -> host | 请求加入游戏 |
| `0x02` | `CMD_REQUESTJOINOK` | `REQUESTJOINOK` | host -> client | 允许加入；payload 常见为 `uint32` |
| `0x03` | `CMD_REQUESTJOIN2` | `REQUESTJOIN2` | client -> host | 加入流程第二阶段 |
| `0x04` | `CMD_PING` | `PING` | both | 空 payload，用于确认和延迟检测 |
| `0x05` | `CMD_PONG` | `PONG` | both | 对 `PING` 的回应 |
| `0x06` | `CMD_PLAYER` | `PLAYER` | host -> client | 通知一个 player record |
| `0x07` | `CMD_ENTER` | `ENTER` | client -> host | 客户端提交玩家名，通常是 cstring |
| `0x08` | `CMD_GAMEDATA` | `GAMEDATA` | host -> client | 分配 player id、最大玩家数、游戏名、statstring 等 |
| `0x09` | `CMD_GAMETYPE` | `GAMETYPE` | host -> client | 游戏类型、ladder/league、slot/race/team 相关字段 |
| `0x0A` | `CMD_JOINFAIL` | `JOINFAIL` | host -> client | 拒绝加入 |
| `0x0B` | `CMD_QUIT` | `QUIT` | both | 离开/断开 |
| `0x0E` | `CMD_GAMESTATE` | `GAMESTATE` | host -> client | 进入游戏状态前的控制信号 |
| `0x0F` | `CMD_STATSCODE` | `STATSCODE` | host -> client | 游戏 stat code，`0` 常见为 Melee |

### GAMEDATA payload

公开资料确认/从资料推断：

| Field | Type | Meaning |
| --- | --- | --- |
| `player_id` | `uint32` | 分配给接收客户端的 player id |
| `max_players` | `uint32` | 最大玩家数 |
| `command2_count` | `uint32` | 公开资料称 command2 count；具体用途需验证 |
| `unknown` | `uint32` | 未知字段 |
| `game_age` | `uint32` | 公开资料称 game age/time |
| `game_name` | `cstring` | 游戏名/创建者显示名 |
| `stat_string` | `cstring` | 游戏属性字符串 |
| `password` | `cstring` | 密码；无密码时可为空串 |

### PLAYER payload

公开资料确认/从资料推断：

| Field | Type | Meaning |
| --- | --- | --- |
| `size` | `uint32` | record payload size 或结构长度 |
| `player_id` | `uint32` | player id |
| `is_host` | `uint32` or flag | 是否为 host/creator |
| `sockaddr` | bytes / unknown | 网络地址；host record 可能为空 |
| `name` | `cstring` | player/account name |
| `stat_string` | `cstring` | player stats；LAN 可为空 |

字段边界在公开资料中不完全清楚。实现时应以抓包中 name 偏移和 size 字段校验。

### GAMETYPE payload

公开资料描述它包含：

- game type
- league/ladder id
- unknown words/bytes
- slot、team、race 或 lobby option 相关字段

不同资料给出的长度和字段拆分不完全一致。实现时应优先复现目标版本抓包，并在文档/代码中标注“抓包常量”。

### STATSCODE payload

| Field | Type | Meaning |
| --- | --- | --- |
| `stat_code` | `uint32` | 游戏统计类型；`0` 常见为 Melee |

### PING/PONG payload

- 常见为空 payload。
- 主要用于保持连接、确认对端存在和更新延迟。

## 加入房间流程

典型 LAN 加入流程：

```text
client -> host: REQUESTJOIN
host   -> client: REQUESTJOINOK
client -> host: REQUESTJOIN2
client -> host: ENTER(name)
host   -> client: GAMEDATA
host   -> client: PLAYER
host   -> client: STATSCODE
host   -> client: GAMETYPE
host   -> client: PING
host   -> client: CLS_SYNC NOP
host   -> client: ROOMDATA / UNKNOWNREQUEST
client -> host: JOINEDGAME
host   -> client: PLAYERJOIN / MAP(kind=0x0001)
client -> host: MAP(kind=0x0000)
host   -> client: MAPPERCENT(100) + SLOTUPDATE + NEWNETPLAYER
client -> host: MAPPERCENT(100)
```

抓包确认：

- `D:\tmp\sc\host2.pcapng`、`client.pcapng`、`client_multi.pcapng` 中，`ROOMDATA` 和 `UNKNOWNREQUEST(0x50)` 出现在 `JOINEDGAME(0x40)` 之前。
- `JOINEDGAME` 是 client -> host 的 `CLS=2` normal 包，不是 `CLS=1`；payload 不只有 `0x40` 一个字节，常见抓包为 `40 00 00 00 00 00 00 00 00 01 00 05 00 00 2f 84 6c d3`，后续字段仍待解释。
- host 在收到 `JOINEDGAME` 后，会向该 joining client 发送 `PLAYERJOIN(player_id)`，然后发送 `MAP(kind=0x0001)`。
- client 对 `MAP(kind=0x0001)` 回 `MAP(kind=0x0000)`；若客户端已有完整地图，payload 中的 file position 等于 map size。
- 在这些抓包中，host 对 `MAP(kind=0x0000)` 的后续推进不是立刻发 `MAP(kind=0x0003)`，而是发 `CLS=2` 的 `MAPPERCENT(100) + SLOTUPDATE + NEWNETPLAYER` 快照；client 随后回 `MAPPERCENT(100)`。
- `MAP(kind=0x0003)` 在 `host2.pcapng`、`client.pcapng` 中出现在 `STARTGAME`/`SEED` 之后的开局转换阶段；不应把它写死为 `MAP(kind=0x0000)` 后立即发送的大厅推进包。

实现注意：

- `ENTER` 之后，host 需要向客户端提供足够的 lobby bootstrap 信息，否则客户端可能停在等待大厅状态。
- `ROOMDATA`、`MAP`、slot state 和 `PLAYERJOIN` 的相对顺序可能影响客户端进入大厅。
- `CLS_SYNC` 即使在大厅阶段也可能需要持续 NOP/slot sync 以维护 turn/reliable 状态。

## SCGP common packets

SCGP 通常放在 `CLS=1` 或 `CLS=2` 的 Storm payload 中，第一个 byte 是 SCGP id。

| SCGP | Constant | Name | Typical CLS | Meaning |
| --- | --- | --- | --- | --- |
| `0x05` | `SCGP_NOP` | `NOP` | `2` | 空同步包/keepalive |
| `0x09` | `SCGP_SELECT` | `SELECT` | `2` | 单位选择 |
| `0x14` | `SCGP_RIGHT_CLICK` | `RIGHT_CLICK` | `2` | 右键命令 |
| `0x1F` | `SCGP_TRAIN` | `TRAIN` | `2` | 训练单位 |
| `0x37` | `SCGP_SYNC` | `SYNC` | `2` | 同步事件；具体 payload 需抓包 |
| `0x3C` | `SCGP_STARTGAME` | `STARTGAME` | `2` | 开始游戏 |
| `0x3D` | `SCGP_MAPPERCENT` | `MAPPERCENT` | `2` | 地图下载/加载百分比 |
| `0x3E` | `SCGP_SLOTUPDATE` | `SLOTUPDATE` | `2` | slot 状态更新 |
| `0x3F` | `SCGP_NEWNETPLAYER` | `NEWNETPLAYER` | `2` | 新网络玩家记录 |
| `0x40` | `SCGP_JOINEDGAME` | `JOINEDGAME` | `2` | 客户端已进入游戏/大厅 |
| `0x41` | `SCGP_CHANGERACE` | `CHANGERACE` | `2` | 玩家改变种族 |
| `0x48` | `SCGP_SEED` | `SEED` | `2` | 开局 seed |
| `0x49` | `SCGP_PLAYERJOIN` | `PLAYERJOIN` | `1` or `2` | 玩家加入事件 |
| `0x4A` | `SCGP_ROOMDATA` | `ROOMDATA` | `1` | 房间 slot/force 数据 |
| `0x4B` | `SCGP_FORCENAMES` | `FORCENAMES` | `1` | force/team 名称 |
| `0x4C` | `SCGP_LOBBYCHAT` | `LOBBYCHAT` | `1` | 大厅聊天 |
| `0x4E` | `SCGP_REJECT` | `REJECT` | `1` | 拒绝/错误 |
| `0x4F` | `SCGP_MAP` | `MAP` | `1` | 地图信息、地图请求、完成信号 |
| `0x50` | `SCGP_UNKNOWNREQUEST` | `UNKNOWNREQUEST` | `1` | 公开资料不完整；抓包中可见 |

### ROOMDATA

公开资料说明 `ROOMDATA` 承载房间内 slot、force/team、玩家状态等信息。字段较复杂，版本和 game type 可能影响布局。

抓包确认的常见布局：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x4A` |
| `tileset` | `uint16` | 地图 tileset / `ERA` |
| `width` | `uint16` | 地图宽度 |
| `height` | `uint16` | 地图高度 |
| `OWNR` | `uint8[12]` | slot owner 状态 |
| `SIDE` | `uint8[12]` | race/side 设置 |
| `OWNR default` | `uint8[12]` | 默认 owner 状态 |
| `FORC` | `uint8[8]` | player -> force/team 映射 |
| `FORC flags` | `uint8[4]` | force flags |
| `RACE` | `uint8[8]` | lobby race 可选/默认信息 |

`(2)Challenger.scm` 抓包中的 `ROOMDATA` 为：

```text
4a
00 00                 # tileset
60 00 80 00           # 96 x 128
06 06 00 00 00 00 00 00 00 00 00 00   # OWNR
06 06 02 01 00 02 01 00 00 00 00 00   # SIDE
06 06 00 00 00 00 00 00 00 00 00 00   # OWNR default
01 01 00 00 00 00 00 00               # FORC
01 00 00 00                           # FORC flags
01 01 00 00 00 00 00 00               # RACE
```

注意：`ROOMDATA` 不一定是 CHK 区段的逐字节拷贝。`(2)Challenger.scm` 的 CHK `FORC` 前 8 字节为 `00 00 00 00 00 00 00 00`，但 LAN 抓包中的 `ROOMDATA.FORC` 是 `01 01 00 00 00 00 00 00`。实现时应优先复现目标版本抓包，而不是直接使用地图 CHK 原值。

实现建议：

- 按目标游戏版本抓包确认二进制布局。
- `ROOMDATA` 描述地图/lobby slot 配置，但不直接携带网络 `player_id`；具体玩家占用关系在 `SLOTUPDATE`/`NEWNETPLAYER` 中更明确。
- active/open/closed/computer/human 状态值需要抓包确认。

### MAP

`SCGP_MAP` 用于地图信息、地图请求和地图传输状态。

常见 map info payload：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x4F` |
| `length` | `uint16` | 后续 event body 长度或相关长度 |
| `kind` | `uint16` | map event kind；`0x0001` 常见为 map info |
| `map_size` | `uint32` | 地图大小 |
| `map_checksum` | `uint32` | 地图 checksum |
| `map_name` | `cstring` | 地图文件名 |

抓包/实现待验证：

- `kind=0x0000` 常见为 client -> host 的地图状态/请求事件。公开资料描述其中包含客户端当前 map file position；当 file position 等于地图大小时，通常可推断客户端已有完整地图。
- `kind=0x0001` 常见为 host -> client 的地图信息/询问事件，包含 map size、checksum 和文件名。客户端随后应以 `kind=0x0000` 回应自己的地图状态。
- `kind=0x0003` 可能表示 map complete。
- `SCGP_MAP` 应按客户端事件响应，不应在同一次处理里无条件连发所有大厅包。抓包显示常见顺序是：`ROOMDATA/UNKNOWNREQUEST` -> `JOINEDGAME` -> `PLAYERJOIN` -> `MAP(kind=0x0001)` -> `MAP(kind=0x0000)` -> `MAPPERCENT/SLOTUPDATE/NEWNETPLAYER`。
- `D:\tmp\sc` 抓包中，`MAP(kind=0x0003)` 出现在开局阶段 `STARTGAME`/`SEED` 后，而不是 `MAP(kind=0x0000)` 的直接响应；该点仍需更多版本抓包确认。
- 客户端可能重复请求地图信息，host 需要按 reliable state、file position 和加载进度响应。

### MAPPERCENT

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x3D` |
| `percent` | `uint8` | `0..100` |

它可用于表示地图加载进度，也可能参与大厅 ready 判断。

抓包确认：host 对 `MAP(kind=0x0000)` 后发送的 `MAPPERCENT(100)` 往往与一整组 `SLOTUPDATE` 和 `NEWNETPLAYER` 合并在同一个 `CLS=2` payload 中；joining client 随后单独回 `MAPPERCENT(100)`。

### SLOTUPDATE

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x3E` |
| `slot` | `uint8` | slot index |
| `player` | `uint8` | player id 或 `0xFF` |
| `state` | `uint8` | slot state |
| `race` | `uint8` | race |
| `team` | `uint8` | team/force |

常见 race 值需要结合抓包确认。实现时不要假定所有 game type 使用同一 slot state 语义。

### NEWNETPLAYER

公开资料不完整。常见抓包布局：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x3F` |
| `player_id` | `uint8` | player id |
| `unknown0` | `uint16` | 待验证 |
| `unknown1` | `uint16` | 待验证 |
| `unknown2` | `uint16` | 待验证 |

抓包常见值为 `unknown0=0`、`unknown1=1`、`unknown2=5`。

### PLAYERJOIN

常见布局：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x49` |
| `player_id` | `uint32` | 加入的 player id |

抓包确认：host 在收到 joining client 的 `JOINEDGAME` 后，会把 `PLAYERJOIN(player_id)` 发给 joining client 本人；因此它不只是“通知其他玩家”的包。

### CHANGERACE

公开资料/抓包显示它用于同步玩家种族变更。常见 payload 至少包含：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x41` |
| `unknown/player/slot` | `uint8` | 待验证 |
| `race` | `uint8` | 新 race |

### STARTGAME

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x3C` |

host 在玩家 ready、地图加载完成后发送。发送后通常还需要一段 NOP/同步过渡，再发送 `SEED`。

### SEED

BNETDocs Redux 说明 `SCGP_SEED` 用于启动随机种子/开局同步。常见布局：

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x48` |
| `seed` | `uint32` | random seed |
| `unknown` | bytes | 公开资料称有附加未知字段；长度需抓包验证 |

实现时应确保所有玩家收到同一 seed。

### LOBBYCHAT

| Field | Type | Meaning |
| --- | --- | --- |
| `id` | `uint8` | `0x4C` |
| `message` | `cstring` | 聊天文本 |

大厅聊天通常走 `CLS=1`，可由 host 转发给其他玩家。

## Statstring

BNETDocs Game Statstrings 描述了 Battle.net/LAN 游戏列表中使用的 statstring。对 `STAR/SEXP`，statstring 通常是逗号分隔字段，尾部含创建者和地图名。

常见结构：

```text
<field0>,<field1>,...,<creator>\r<map title or map name>\r
```

常见字段语义包括：

- 地图 tileset / map dimension。
- game type。
- game speed。
- game visibility。
- race/team/lock teams 等 option flags。
- map checksum 或 hash。
- creator name。
- map display name。

实现注意：

- statstring 字段数和含义会随 product、版本、game type 变化。
- map checksum/hash 必须和客户端可识别的地图信息一致，否则可能导致房间显示异常或地图请求异常。
- creator/map 尾部字段使用 carriage return (`\r`) 分隔。

## 已知不确定点

- `GAMETYPE` payload 的字段拆分和长度在公开资料中不完全一致；应以目标版本抓包为准。
- `ROOMDATA` 的完整 slot/force 布局需要更多抓包确认。
- `NEWNETPLAYER` 后续 word 字段语义未完全确认。
- `STATUS_RESEND_REQUEST` 的 payload/callback 语义不完整；不要把 payload 前两个字节固定解释为 requested seq。
- `STATUS_RESEND_RESPONSE/CALLBACK` 的命名和行为在不同资料中可能不一致。
- LAN discovery `state` bit 的完整含义需要抓包确认。
- 地图 checksum 与 `SCGP_MAP` 中 checksum/hash 的来源需结合 CHK/MPQ 或抓包验证。

## 实现检查清单

- 所有 outgoing Storm normal 包按 `CLS` 分别维护 `Seq1` 发送计数。
- 所有 incoming normal 包按 `CLS` 更新 `Seq2`/last received 计数。
- 每个 `CLS` 单独保存最近发送历史，用于 resend。
- 处理 resend 时记录 header 和 payload 原始信息，避免错误解释 callback 字段。
- `CLS=2` 同步包需要稳定 cadence；大厅阶段也可能需要 NOP 或 slot sync。
- 任何抓包常量进入代码时，应在注释中说明“抓包常量”而非协议确定字段。
