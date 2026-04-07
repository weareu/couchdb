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
import { Table } from 'react-bootstrap';
import { formatBytes, formatMs } from './formatters';

export default class StatusPanel extends React.Component {
  statusText = () => {
    const { enabled, paused } = this.props.status;
    if (!enabled) return 'Disabled';
    if (paused) return 'Paused';
    return 'Active';
  };

  render() {
    const status = this.props.status;
    return (
      <div className="autoshard-section">
        <h3>Status</h3>
        <Table striped className="table-autoshard-status">
          <tbody>
            <tr>
              <td>State</td>
              <td>{this.statusText()}</td>
            </tr>
            <tr>
              <td>Coordinator on this node</td>
              <td>{status.is_coordinator ? 'Yes' : 'No'}</td>
            </tr>
            <tr>
              <td>Maximum shard size</td>
              <td>{formatBytes(status.max_shard_size_bytes)}</td>
            </tr>
            <tr>
              <td>Scan interval</td>
              <td>{formatMs(status.scan_interval_ms)}</td>
            </tr>
            <tr>
              <td>Maximum concurrent splits</td>
              <td>{status.max_concurrent_splits}</td>
            </tr>
            <tr>
              <td>Maintenance window</td>
              <td>{status.maintenance_window === 'always' ? 'Always' : status.maintenance_window}</td>
            </tr>
            <tr>
              <td>Active splits</td>
              <td>{status.active_splits}</td>
            </tr>
            <tr>
              <td>Cooldowns active</td>
              <td>{status.cooldowns_active}</td>
            </tr>
            <tr>
              <td>Space reserved by auto-split</td>
              <td>{formatBytes(status.space_reserved_bytes || 0)}</td>
            </tr>
            <tr>
              <td>Scans completed</td>
              <td>{status.scan_count}</td>
            </tr>
            <tr>
              <td>Splits triggered</td>
              <td>{status.splits_triggered}</td>
            </tr>
            <tr>
              <td>Excluded databases</td>
              <td>{(status.exclude_patterns || []).join(', ') || '(none)'}</td>
            </tr>
          </tbody>
        </Table>
      </div>
    );
  }
}
