// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

import React from 'react';
import { Form, InputGroup, Button } from 'react-bootstrap';
import ReactComponents from "../../components/react-components";

const ConfirmButton = ReactComponents.ConfirmButton;

export default class ControlsPanel extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      thresholdGB: this.gbFromBytes(props.status.max_shard_size_bytes)
    };
  }

  gbFromBytes = (bytes) => {
    if (!bytes) return '20';
    return String(Math.round(bytes / 1e9));
  };

  componentDidUpdate(prevProps) {
    if (prevProps.status.max_shard_size_bytes !== this.props.status.max_shard_size_bytes) {
      this.setState({ thresholdGB: this.gbFromBytes(this.props.status.max_shard_size_bytes) });
    }
  }

  onChangeThreshold = (e) => {
    this.setState({ thresholdGB: e.target.value });
  };

  onApplyThreshold = (e) => {
    e.preventDefault();
    const bytes = parseInt(this.state.thresholdGB, 10) * 1e9;
    if (bytes > 0) {
      this.props.updateThreshold(bytes);
    }
  };

  onToggleEnabled = (e) => {
    e.preventDefault();
    this.props.toggleEnabled(this.props.status.enabled);
  };

  onTogglePause = (e) => {
    e.preventDefault();
    this.props.togglePause(this.props.status.paused);
  };

  onTriggerScan = (e) => {
    e.preventDefault();
    this.props.doTriggerScan();
  };

  render() {
    const { enabled, paused } = this.props.status;
    return (
      <div className="autoshard-section">
        <h3>Controls</h3>
        <div className="autoshard-controls">
          <Button
            variant={enabled ? 'cf-danger' : 'cf-primary'}
            onClick={this.onToggleEnabled}
          >
            <i className={'icon ' + (enabled ? 'fonticon-cancel' : 'fonticon-ok-circled')} />
            {enabled ? 'Disable Auto-Split' : 'Enable Auto-Split'}
          </Button>
          {' '}
          <Button
            variant="cf-secondary"
            onClick={this.onTogglePause}
            disabled={!enabled}
          >
            <i className={'icon ' + (paused ? 'fonticon-play' : 'fonticon-pause')} />
            {paused ? 'Resume' : 'Pause'}
          </Button>
          {' '}
          <Button
            variant="cf-secondary"
            onClick={this.onTriggerScan}
            disabled={!enabled || paused}
          >
            <i className="icon fonticon-refresh" />
            Trigger Scan
          </Button>
        </div>

        <Form onSubmit={this.onApplyThreshold} className="autoshard-threshold-form">
          <Form.Label htmlFor="autoshard-threshold-input">
            Maximum shard size (GB)
          </Form.Label>
          <InputGroup id="autoshard-threshold-group">
            <Form.Control
              id="autoshard-threshold-input"
              type="number"
              min="1"
              value={this.state.thresholdGB}
              onChange={this.onChangeThreshold}
            />
            <ConfirmButton
              text="Update"
              onClick={this.onApplyThreshold}
              showIcon={false}
            />
          </InputGroup>
          <Form.Text>
            Shards larger than this will be auto-split.
          </Form.Text>
        </Form>
      </div>
    );
  }
}
