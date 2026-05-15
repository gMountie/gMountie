# Server Configuration

The gMountie server configuration file uses YAML format and supports various
options for customizing server behavior, authentication, and volume management.

## Configuration File Structure

The configuration file has three main sections:

- Server configuration
- Authentication configuration
- Volumes configuration

Basic example:

```yaml
server:
  address: 0.0.0.0
  port: 9449
authentication:
  type: none
volumes:
  - name: shared
    path: /shared
```

## Server Options

The `server` section configures the core server settings:

| Option              | Type     | Default      | Description                                                |
|---------------------|----------|--------------|------------------------------------------------------------|
| address             | string   | "0\.0\.0\.0" | IP address the server listens on                           |
| port                | integer  | 9449         | Port number for the gRPC server                            |
| metrics             | boolean  | true         | Enable/disable Prometheus metrics                          |
| metrics\_addr       | string   | ":9090"      | Address the ops HTTP server listens on                     |
| max\_message\_bytes | integer  | 16777216     | Cap on inbound/outbound gRPC message size (16 MiB default) |

`max_message_bytes` is validated to the range [65536, 67108864] (64 KiB to
64 MiB). The default sits well above the streaming `frame_size_bytes` so a
single Read/Write frame plus header overhead always fits.

Example:

```yaml
server:
  address: 192.168.1.100 # Listen on specific interface
  port: 8080 # Custom port
  metrics: false # Disable metrics
  max_message_bytes: 33554432 # 32 MiB
```

### Keepalive

The `server.keepalive` block tunes gRPC HTTP/2 keepalive pings. Defaults
make the server ping idle connections every 30s and tear them down 10s
after a missed ACK, so a dead client (or a half-open NAT path) surfaces
within ~40s instead of waiting on TCP timeouts.

| Option                          | Type     | Default | Description                                                      |
|---------------------------------|----------|---------|------------------------------------------------------------------|
| time                            | duration | 30s     | Interval between pings to an idle connection                     |
| timeout                         | duration | 10s     | Wait time for a ping ACK before closing the connection           |
| min\_time                       | duration | 10s     | Minimum interval the server tolerates between client pings       |
| permit\_without\_stream         | boolean  | true    | Allow client pings when no streams are in flight                 |

Example:

```yaml
server:
  keepalive:
    time: 15s
    timeout: 5s
    min_time: 5s
    permit_without_stream: true
```

## Authentication Options

The `auth` section configures user authentication:

| Option | Type   | Required        | Description                             |
|--------|--------|-----------------|-----------------------------------------|
| type   | string | yes             | Authentication type ("none" or "basic") |
| users  | array  | yes (for basic) | List of user credentials                |

### None Authentication

Disables authentication (not recommended for production):

```yaml
auth:
  type: none
```

### Basic Authentication

Enables username/password authentication:

```yaml
auth:
  type: basic
  users:
    - username: admin
      password: admin
    - username: user1
      password: pass123
```

## Volume Configuration

The `volumes` section defines shared directories:

| Option | Type   | Required | Description                       |
|--------|--------|----------|-----------------------------------|
| name   | string | yes      | Unique volume identifier          |
| path   | string | yes      | Absolute path to shared directory |

Example with multiple volumes:

```yaml
volumes:
  - name: documents
    path: /srv/documents
  - name: media
    path: /srv/media
  - name: backup
    path: /srv/backup
```

## Example Configuration

Here's an example configuration file that enables basic authentication and
exposes two volumes:

```yaml
server:
  address: 0.0.0.0
  port: 9449
  metrics: true
authentication:
  type: basic
  users:
    - username: admin
      password: admin
volumes:
  - name: shared
    path: /shared
  - name: private
    path: /private
```
