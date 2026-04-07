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

import Helpers from "../../helpers";
import { get, put, post } from '../../core/ajax';

export const fetchAutoShardStatus = () => {
  return get(Helpers.getServerUrl('/_reshard/auto'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

export const updateAutoShardConfig = (config) => {
  return put(Helpers.getServerUrl('/_reshard/auto'), config)
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

export const triggerScan = () => {
  return post(Helpers.getServerUrl('/_reshard/auto/scan'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

export const pauseAutoShard = () => {
  return post(Helpers.getServerUrl('/_reshard/auto/pause'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

export const resumeAutoShard = () => {
  return post(Helpers.getServerUrl('/_reshard/auto/resume'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

// Fetch all space reservations from couch_space_monitor (auto-split,
// manual reshard, smoosh compaction, manual compact)
export const fetchSpaceReservations = () => {
  return get(Helpers.getServerUrl('/_reshard/space'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};

// Fetch all active tasks (splits + compactions) for unified progress view
export const fetchActiveTasks = () => {
  return get(Helpers.getServerUrl('/_active_tasks'))
    .then(resp => {
      if (resp.error) throw new Error(resp.reason);
      return resp;
    });
};
