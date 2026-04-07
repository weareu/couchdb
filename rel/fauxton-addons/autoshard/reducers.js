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

import ActionTypes from './actiontypes';

const initialState = {
  isLoading: true,
  error: null,
  status: {
    enabled: false,
    paused: false,
    max_shard_size_bytes: 20000000000,
    scan_interval_ms: 600000,
    max_concurrent_splits: 2,
    active_splits: 0,
    active_split_shards: [],
    cooldowns_active: 0,
    space_reserved_bytes: 0,
    space_reservations: {},
    scan_count: 0,
    splits_triggered: 0,
    is_coordinator: false,
    maintenance_window: 'always',
    exclude_patterns: []
  },
  space: {
    total_reserved_bytes: 0,
    reservation_count: 0,
    by_node: {},
    reservations: []
  },
  tasks: []
};

export default function autoshard(state = initialState, action) {
  switch (action.type) {
    case ActionTypes.AUTOSHARD_SET_STATUS:
      return { ...state, status: action.status, error: null };
    case ActionTypes.AUTOSHARD_SET_LOADING:
      return { ...state, isLoading: action.isLoading };
    case ActionTypes.AUTOSHARD_SET_ERROR:
      return { ...state, error: action.error, isLoading: false };
    case ActionTypes.AUTOSHARD_SET_SPACE:
      return { ...state, space: action.space };
    case ActionTypes.AUTOSHARD_SET_TASKS:
      return { ...state, tasks: action.tasks };
    default:
      return state;
  }
}

export const getStatus = (state) => state.status;
export const getIsLoading = (state) => state.isLoading;
export const getError = (state) => state.error;
export const getSpace = (state) => state.space;
export const getTasks = (state) => state.tasks;
