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

import React, { useEffect, useState } from 'react';
import { Badge, Button, Card, Col, Form, Row, Spinner, Table } from 'react-bootstrap';

function formatBytes(bytes) {
  if (bytes >= 1e12) return (bytes / 1e12).toFixed(1) + ' TB';
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  return bytes + ' B';
}

function formatMs(ms) {
  if (ms >= 3600000) return (ms / 3600000).toFixed(1) + 'h';
  if (ms >= 60000) return (ms / 60000).toFixed(0) + 'min';
  if (ms >= 1000) return (ms / 1000).toFixed(0) + 's';
  return ms + 'ms';
}

function StatusBadge({ enabled, paused }) {
  if (!enabled) return <Badge bg="secondary">Disabled</Badge>;
  if (paused) return <Badge bg="warning">Paused</Badge>;
  return <Badge bg="success">Active</Badge>;
}

export default function AutoShardLayout({
  status, isLoading, error, space, tasks,
  loadStatus, toggleEnabled, togglePause, doTriggerScan, updateThreshold
}) {
  const [thresholdGB, setThresholdGB] = useState('');

  useEffect(() => {
    loadStatus();
    // Refresh every 5s for live progress on active tasks + space
    const interval = setInterval(() => loadStatus(), 5000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    if (status.max_shard_size_bytes) {
      setThresholdGB((status.max_shard_size_bytes / 1e9).toFixed(0));
    }
  }, [status.max_shard_size_bytes]);

  if (isLoading && !status.enabled && status.scan_count === 0) {
    return (
      <div className="autoshard-page" style={{ padding: '20px' }}>
        <Spinner animation="border" /> Loading auto-shard status...
      </div>
    );
  }

  if (error) {
    return (
      <div className="autoshard-page" style={{ padding: '20px' }}>
        <div className="alert alert-danger">
          Failed to load auto-shard status: {error}
        </div>
      </div>
    );
  }

  return (
    <div className="autoshard-page" style={{ padding: '20px' }}>
      <h2>Auto-Shard Splitting</h2>
      <p className="text-muted">
        Automatically splits oversized shards to prevent slow compaction,
        massive index rebuilds, and replication bottlenecks.
      </p>

      <Row className="mb-4">
        <Col md={8}>
          <Card>
            <Card.Header>
              <span style={{ fontWeight: 'bold' }}>Status</span>
              {' '}
              <StatusBadge enabled={status.enabled} paused={status.paused} />
              {status.is_coordinator &&
                <Badge bg="info" className="ms-2">Coordinator</Badge>
              }
            </Card.Header>
            <Card.Body>
              <Table size="sm" borderless>
                <tbody>
                  <tr>
                    <td style={{ width: '220px' }}>Max shard size</td>
                    <td><strong>{formatBytes(status.max_shard_size_bytes)}</strong></td>
                  </tr>
                  <tr>
                    <td>Scan interval</td>
                    <td>{formatMs(status.scan_interval_ms)}</td>
                  </tr>
                  <tr>
                    <td>Max concurrent splits</td>
                    <td>{status.max_concurrent_splits}</td>
                  </tr>
                  <tr>
                    <td>Maintenance window</td>
                    <td>{status.maintenance_window === 'always' ? 'Always' : status.maintenance_window}</td>
                  </tr>
                  <tr>
                    <td>Excluded databases</td>
                    <td>
                      {(status.exclude_patterns || []).map((p, i) =>
                        <Badge key={i} bg="secondary" className="me-1">{p}</Badge>
                      )}
                    </td>
                  </tr>
                </tbody>
              </Table>
            </Card.Body>
          </Card>
        </Col>

        <Col md={4}>
          <Card>
            <Card.Header style={{ fontWeight: 'bold' }}>Statistics</Card.Header>
            <Card.Body>
              <Table size="sm" borderless>
                <tbody>
                  <tr>
                    <td>Active splits</td>
                    <td>
                      <strong>{status.active_splits}</strong>
                      {status.active_splits > 0 &&
                        <Badge bg="warning" className="ms-2">Running</Badge>
                      }
                    </td>
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
                    <td>Cooldowns active</td>
                    <td>{status.cooldowns_active}</td>
                  </tr>
                </tbody>
              </Table>
            </Card.Body>
          </Card>
        </Col>
      </Row>

      {/* Active tasks (splits + compactions in one place) */}
      {tasks && tasks.length > 0 && (
        <Card className="mb-4">
          <Card.Header style={{ fontWeight: 'bold' }}>
            Active Tasks
            <Badge bg="info" className="ms-2">{tasks.length}</Badge>
          </Card.Header>
          <Card.Body>
            <Table striped size="sm">
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Database</th>
                  <th>Phase</th>
                  <th>Progress</th>
                </tr>
              </thead>
              <tbody>
                {tasks.map((task, i) => (
                  <tr key={i}>
                    <td>
                      <Badge bg={task.type === 'shard_split' ? 'primary' : 'secondary'}>
                        {task.type}
                      </Badge>
                    </td>
                    <td><code>{task.database || '-'}</code></td>
                    <td>{task.phase || '-'}</td>
                    <td>
                      <div className="progress" style={{ height: '18px', minWidth: '120px' }}>
                        <div
                          className="progress-bar"
                          role="progressbar"
                          style={{ width: (task.progress || 0) + '%' }}
                          aria-valuenow={task.progress || 0}
                          aria-valuemin="0"
                          aria-valuemax="100"
                        >
                          {task.progress || 0}%
                        </div>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </Card.Body>
        </Card>
      )}

      {/* Space reservations from couch_space_monitor */}
      {space && space.reservation_count > 0 && (
        <Card className="mb-4">
          <Card.Header style={{ fontWeight: 'bold' }}>
            Space Reservations
            <Badge bg="warning" className="ms-2">
              {formatBytes(space.total_reserved_bytes)} total
            </Badge>
          </Card.Header>
          <Card.Body>
            <p className="text-muted">
              Cluster-wide disk space reserved by all in-flight operations
              (auto-split, manual reshard, smoosh compaction, manual compact).
              Reservations prevent thundering herd when many operations
              run concurrently.
            </p>
            <Table striped size="sm">
              <thead>
                <tr>
                  <th>Operation</th>
                  <th>Node</th>
                  <th>Reserved</th>
                </tr>
              </thead>
              <tbody>
                {space.reservations.map((r, i) => (
                  <tr key={i}>
                    <td>{r.description}</td>
                    <td><code>{r.node}</code></td>
                    <td>{formatBytes(r.bytes)}</td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </Card.Body>
        </Card>
      )}

      {/* Controls */}
      <Card>
        <Card.Header style={{ fontWeight: 'bold' }}>Controls</Card.Header>
        <Card.Body>
          <Row className="mb-3">
            <Col md={3}>
              <Button
                variant={status.enabled ? 'danger' : 'success'}
                onClick={() => toggleEnabled(status.enabled)}
                className="w-100"
              >
                {status.enabled ? 'Disable Auto-Split' : 'Enable Auto-Split'}
              </Button>
            </Col>
            <Col md={3}>
              <Button
                variant={status.paused ? 'success' : 'warning'}
                onClick={() => togglePause(status.paused)}
                disabled={!status.enabled}
                className="w-100"
              >
                {status.paused ? 'Resume' : 'Pause'}
              </Button>
            </Col>
            <Col md={3}>
              <Button
                variant="info"
                onClick={doTriggerScan}
                disabled={!status.enabled || status.paused}
                className="w-100"
              >
                Trigger Scan Now
              </Button>
            </Col>
          </Row>

          <Row>
            <Col md={6}>
              <Form.Group>
                <Form.Label>Max shard size (GB)</Form.Label>
                <div className="d-flex">
                  <Form.Control
                    type="number"
                    value={thresholdGB}
                    onChange={(e) => setThresholdGB(e.target.value)}
                    min="1"
                    style={{ maxWidth: '120px' }}
                  />
                  <Button
                    variant="outline-primary"
                    className="ms-2"
                    onClick={() => {
                      const bytes = parseInt(thresholdGB, 10) * 1e9;
                      if (bytes > 0) updateThreshold(bytes);
                    }}
                  >
                    Update
                  </Button>
                </div>
                <Form.Text className="text-muted">
                  Shards larger than this will be auto-split
                </Form.Text>
              </Form.Group>
            </Col>
          </Row>
        </Card.Body>
      </Card>
    </div>
  );
}
