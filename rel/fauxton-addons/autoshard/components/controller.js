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
import StatusPanel from './statuspanel';
import ControlsPanel from './controlspanel';
import ActiveTasksTable from './activetaskstable';
import ReservationsTable from './reservationstable';

export default class AutoShardController extends React.Component {
  componentDidMount() {
    this.props.loadStatus();
    this.pollInterval = setInterval(this.props.loadStatus, 5000);
  }

  componentWillUnmount() {
    if (this.pollInterval) {
      clearInterval(this.pollInterval);
    }
  }

  render() {
    const {
      status,
      isLoading,
      error,
      space,
      tasks,
      toggleEnabled,
      togglePause,
      doTriggerScan,
      updateThreshold
    } = this.props;

    if (isLoading && status.scan_count === 0) {
      return (
        <div id="autoshard-page" className="scrollable">
          <div className="inner">
            <p>Loading auto-shard status...</p>
          </div>
        </div>
      );
    }

    if (error) {
      return (
        <div id="autoshard-page" className="scrollable">
          <div className="inner">
            <div className="errors-container">
              <p>Failed to load auto-shard status: {error}</p>
            </div>
          </div>
        </div>
      );
    }

    return (
      <div id="autoshard-page" className="scrollable">
        <div className="inner">
          <StatusPanel status={status} />

          <ControlsPanel
            status={status}
            toggleEnabled={toggleEnabled}
            togglePause={togglePause}
            doTriggerScan={doTriggerScan}
            updateThreshold={updateThreshold}
          />

          <ActiveTasksTable tasks={tasks || []} />

          <ReservationsTable space={space || {reservations: [], total_reserved_bytes: 0}} />
        </div>
      </div>
    );
  }
}
