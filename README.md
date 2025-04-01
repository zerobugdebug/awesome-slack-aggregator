# Awesome Slack Aggregator

A powerful Slack messages aggregator that collects messages from multiple channels and threads into a single unified feed.

## Project Description

Awesome Slack Aggregator is a Go application that monitors multiple Slack channels and conversations, aggregating messages into a single feed and forwarding them to a target destination. It's designed for Slack power users who need to keep track of conversations across numerous channels without constantly switching contexts.

### Why It Was Created

This tool was created to solve the problem of information overload and context switching when participating in a busy Slack workspace with many active channels. By aggregating messages into a single feed, you can:

- Stay informed about conversations across multiple channels without having to check each one individually
- Monitor important threads for new activity without manually checking them
- Reduce the chances of missing important messages
- Create a searchable personal archive of important conversations

### Benefits

- **Unified Message Feed**: Consolidates messages from many channels into a single feed
- **Thread Tracking**: Automatically monitors thread replies so you don't miss conversation updates
- **State Persistence**: Maintains state between runs, so you won't miss messages when restarting
- **Message Retention**: Configurable message retention period with automatic cleanup
- **Channel Support**: Works with all Slack channel types (public, private, DMs, multi-person DMs)
- **Message Batching**: Intelligently batches messages to avoid flooding
- **Detailed Logging**: Comprehensive logging for troubleshooting and monitoring

### Drawbacks

- **Permissions**: Requires a Slack user token with appropriate permissions
- **Rate Limiting**: May encounter Slack API rate limits with many active channels
- **Notification Management**: Could potentially create notification noise for the target user
- **Permission Requirements**: For full functionality (e.g., opening DMs), requires specific permissions like `im:write`

## Installation

```bash
git clone https://github.com/zerobugdebug/awesome-slack-aggregator.git
cd awesome-slack-aggregator
go build
```

## How to Use

### Requirements

1. A Slack user token with appropriate permissions
2. Go 1.21 or later (required for `maps` package)

### Environment Variables

Set these environment variables before running the application:

- `SLACK_USER_TOKEN`: Your Slack user token (required)
- `SLACK_TARGET_USER_ID`: The Slack user ID to send aggregated messages to (optional)
  - Set to `self` to send messages to your Slackbot DM
  - Leave empty to only log messages to console

### Command Line Flags

```bash
./awesome-slack-aggregator [flags]
```

Available flags:

- `--log-level`: Logging level (default: "info")
  - Options: trace, debug, info, warn, error, fatal, panic
- `--state-dir`: Directory for persistent state storage (default: current directory)
- `--retention`: Number of days to retain messages before deletion (default: 7)

### Examples

Basic usage, sending messages to yourself:

```bash
export SLACK_USER_TOKEN=xoxp-your-token-here
export SLACK_TARGET_USER_ID=self
./awesome-slack-aggregator
```

Advanced usage with custom settings:

```bash
export SLACK_USER_TOKEN=xoxp-your-token-here
export SLACK_TARGET_USER_ID=U012345ABC
./awesome-slack-aggregator --log-level=debug --state-dir=/var/lib/slack-feed --retention=14
```

## File Structure

The application creates and maintains these files:

- `<state-dir>/slack-feed.state`: JSON file containing the application state, including:
  - Latest message timestamps for each channel
  - Active threads being monitored
  - Sent messages for retention management

## Obtaining a Slack Token

To use this application, you need a Slack user token with the following scopes:

- `channels:history` - Read messages from public channels
- `groups:history` - Read messages from private channels
- `im:history` - Read messages from direct messages
- `mpim:history` - Read messages from multi-person direct messages
- `channels:read` - View basic information about public channels
- `groups:read` - View basic information about private channels
- `im:read` - View basic information about direct messages
- `mpim:read` - View basic information about multi-person direct messages
- `users:read` - View basic information about users
- `chat:write` - Send messages
- `im:write` - (Optional) Open direct messages with people

You can create a Slack app and obtain a token at [api.slack.com](https://api.slack.com/apps).

## Warning Notes

### Potential Issues

1. **Rate Limiting**: 
   - With many channels, you may encounter Slack API rate limits
   - Adjust polling intervals if necessary in the code if you have a large workspace

2. **Message Flooding**:
   - The application batches messages to prevent flooding
   - Default batching is 5 messages or 10 seconds between batches

3. **Feedback Loops**:
   - The app avoids monitoring DMs with the target user to prevent feedback loops
   - Ensure your target user ID is correctly set

4. **Message Retention**:
   - By default, aggregated messages are deleted after 7 days
   - Adjust the retention period with the `--retention` flag if needed

5. **Permissions**:
   - If the app can't open a DM with the target user, ensure you have the `im:write` scope
   - Without `im:write`, the app will fall back to only logging messages to the console

6. **State Management**:
   - The app maintains state in the specified directory
   - Ensure the directory is writable by the user running the application

## Dependencies

- [github.com/slack-go/slack](https://github.com/slack-go/slack) - Slack API client
- [github.com/rs/zerolog](https://github.com/rs/zerolog) - Zero allocation JSON logger

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
