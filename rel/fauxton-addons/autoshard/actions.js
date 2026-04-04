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

import FauxtonAPI from '../../core/api';
import ActionTypes from './actiontypes';
import {
  fetchAutoShardStatus,
  updateAutoShardConfig,
  triggerScan,
  pauseAutoShard,
  resumeAutoShard
} from './api';

export const setLoading = (isLoading) => ({
  type: ActionTypes.AUTOSHARD_SET_LOADING,
  isLoading
});

export const setStatus = (status) => ({
  type: ActionTypes.AUTOSHARD_SET_STATUS,
  status
});

export const setError = (error) => ({
  type: ActionTypes.AUTOSHARD_SET_ERROR,
  error
});

export const loadStatus = () => (dispatch) => {
  dispatch(setLoading(true));
  fetchAutoShardStatus()
    .then(status => {
      dispatch(setLoading(false));
      dispatch(setStatus(status));
    })
    .catch(err => {
      dispatch(setLoading(false));
      dispatch(setError(err.message));
    });
};

export const refreshStatus = () => (dispatch) => {
  fetchAutoShardStatus()
    .then(status => dispatch(setStatus(status)))
    .catch(() => {});
};

export const toggleEnabled = (currentlyEnabled) => (dispatch) => {
  const newEnabled = !currentlyEnabled;
  updateAutoShardConfig({ enabled: newEnabled })
    .then(() => {
      FauxtonAPI.addNotification({
        msg: `Auto-shard splitting ${newEnabled ? 'enabled' : 'disabled'}`,
        type: 'success'
      });
      dispatch(loadStatus());
    })
    .catch(err => {
      FauxtonAPI.addNotification({
        msg: `Failed to update auto-shard: ${err.message}`,
        type: 'error'
      });
    });
};

export const togglePause = (currentlyPaused) => (dispatch) => {
  const action = currentlyPaused ? resumeAutoShard : pauseAutoShard;
  action()
    .then(() => {
      FauxtonAPI.addNotification({
        msg: currentlyPaused ? 'Auto-shard resumed' : 'Auto-shard paused',
        type: 'success'
      });
      dispatch(loadStatus());
    })
    .catch(err => {
      FauxtonAPI.addNotification({
        msg: `Failed: ${err.message}`,
        type: 'error'
      });
    });
};

export const doTriggerScan = () => (dispatch) => {
  triggerScan()
    .then(() => {
      FauxtonAPI.addNotification({
        msg: 'Scan triggered',
        type: 'success'
      });
      setTimeout(() => dispatch(loadStatus()), 2000);
    })
    .catch(err => {
      FauxtonAPI.addNotification({
        msg: `Scan failed: ${err.message}`,
        type: 'error'
      });
    });
};

export const updateThreshold = (bytes) => (dispatch) => {
  updateAutoShardConfig({ max_shard_size_bytes: bytes })
    .then(() => {
      FauxtonAPI.addNotification({
        msg: `Threshold updated to ${formatBytes(bytes)}`,
        type: 'success'
      });
      dispatch(loadStatus());
    })
    .catch(err => {
      FauxtonAPI.addNotification({
        msg: `Failed: ${err.message}`,
        type: 'error'
      });
    });
};

function formatBytes(bytes) {
  if (bytes >= 1e12) return (bytes / 1e12).toFixed(1) + ' TB';
  if (bytes >= 1e9) return (bytes / 1e9).toFixed(1) + ' GB';
  if (bytes >= 1e6) return (bytes / 1e6).toFixed(1) + ' MB';
  return bytes + ' bytes';
}
