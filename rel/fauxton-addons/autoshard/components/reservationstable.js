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
import { formatBytes } from './formatters';

export default class ReservationsTable extends React.Component {
  renderRow = (r, key) => {
    return (
      <tr key={key}>
        <td>{r.description}</td>
        <td>{r.node}</td>
        <td>{formatBytes(r.bytes)}</td>
      </tr>
    );
  };

  renderEmpty = () => {
    return (
      <tr className="no-matching-database-on-search">
        <td colSpan="3">No active disk space reservations.</td>
      </tr>
    );
  };

  render() {
    const space = this.props.space || {};
    const reservations = space.reservations || [];
    return (
      <div className="autoshard-section">
        <h3>Disk Space Reservations</h3>
        <p>
          Total reserved cluster-wide: <strong>{formatBytes(space.total_reserved_bytes || 0)}</strong>
          {' '}({reservations.length} active)
        </p>
        <div id="dashboard-lower-content">
          <Table striped className="table-autoshard-reservations">
            <thead>
              <tr>
                <th>Operation</th>
                <th>Node</th>
                <th>Reserved</th>
              </tr>
            </thead>
            <tbody>
              {reservations.length === 0
                ? this.renderEmpty()
                : reservations.map((r, i) => this.renderRow(r, i))
              }
            </tbody>
          </Table>
        </div>
      </div>
    );
  }
}
