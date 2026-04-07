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

export default class ActiveTasksTable extends React.Component {
  renderRow = (task, key) => {
    const progress = (task.progress != null) ? task.progress : 0;
    return (
      <tr key={key}>
        <td>{task.type}</td>
        <td>{task.database || ''}</td>
        <td>{task.phase || ''}</td>
        <td>{progress}%</td>
        <td>{(task.changes_done != null) ? task.changes_done : ''}</td>
        <td>{(task.total_changes != null) ? task.total_changes : ''}</td>
        <td>{task.node || ''}</td>
      </tr>
    );
  };

  renderEmpty = () => {
    return (
      <tr className="no-matching-database-on-search">
        <td colSpan="7">No active tasks.</td>
      </tr>
    );
  };

  render() {
    const tasks = this.props.tasks || [];
    return (
      <div className="autoshard-section">
        <h3>Active Tasks</h3>
        <div id="dashboard-lower-content">
          <Table striped className="table-autoshard-tasks">
            <thead>
              <tr>
                <th>Type</th>
                <th>Database</th>
                <th>Phase</th>
                <th>Progress</th>
                <th>Changes done</th>
                <th>Total changes</th>
                <th>Node</th>
              </tr>
            </thead>
            <tbody>
              {tasks.length === 0
                ? this.renderEmpty()
                : tasks.map((task, i) => this.renderRow(task, i))
              }
            </tbody>
          </Table>
        </div>
      </div>
    );
  }
}
