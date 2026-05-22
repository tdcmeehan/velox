/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <atomic>

#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/ArbitratorTestUtil.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/QueryAssertions.h"

namespace facebook::velox::exec::test {
namespace {

class HashJoinBridgeCallbackTest : public HiveConnectorTestBase {
 protected:
  void SetUp() override {
    HiveConnectorTestBase::SetUp();
    rowType_ = ROW({"c0", "c1"}, {BIGINT(), BIGINT()});
  }

  std::vector<RowVectorPtr> makeProbeAndBuildVectors() {
    return {
        makeRowVector(
            {"c0", "c1"},
            {
                makeFlatVector<int64_t>(
                    256, [](auto row) { return row % 64; }),
                makeFlatVector<int64_t>(
                    256, [](auto row) { return row; }),
            }),
    };
  }

  RowTypePtr rowType_;
};

// Verifies that the bridge callback registered via
// Task::registerHashJoinBridgeCallback fires from a suspended driver. The
// suspended-driver invariant is what allows the callback to safely allocate
// from arbitrator-tracked task-child memory pools: the firing site wraps the
// invocation in enterSuspended/leaveSuspended so any allocation that
// triggers arbitration finds this driver already off the on-thread set,
// avoiding the deadlock where Task::MemoryReclaimer::requestPause().wait()
// blocks waiting for the very driver that is synchronously firing the
// callback.
TEST_F(HashJoinBridgeCallbackTest, callbackFiresFromSuspendedDriver) {
  auto vectors = makeProbeAndBuildVectors();

  core::PlanNodeId joinNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors, true)
          .hashJoin(
              {"c0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator)
                  .values(vectors, true)
                  .project({"c0 AS u0", "c1 AS u1"})
                  .planNode(),
              "",
              {"c0", "c1"},
              core::JoinType::kInner)
          .capturePlanNodeId(joinNodeId)
          .planFragment();

  auto queryPool = memory::memoryManager()->addRootPool(
      "callbackFiresFromSuspendedDriver",
      1UL << 30,
      exec::MemoryReclaimer::create());
  auto queryCtx = core::QueryCtx::create(
      driverExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      std::move(queryPool),
      nullptr);

  auto task = Task::create(
      "callback-suspended-driver",
      std::move(plan),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel,
      [](RowVectorPtr /*data*/,
         bool /*drained*/,
         ContinueFuture* /*future*/) { return BlockingReason::kNotBlocked; });

  std::atomic<bool> callbackFired{false};
  std::atomic<bool> sawSuspendedDriver{false};
  task->registerHashJoinBridgeCallback(
      joinNodeId,
      [&](const BaseHashTable& /*mainTable*/,
          const std::vector<std::unique_ptr<BaseHashTable>>& /*otherTables*/,
          bool /*hasNullKeys*/) {
        callbackFired = true;
        auto* threadCtx = driverThreadContext();
        ASSERT_NE(threadCtx, nullptr)
            << "bridge callback must run on a driver thread";
        auto* driver = threadCtx->driverCtx()->driver;
        ASSERT_NE(driver, nullptr);
        sawSuspendedDriver = driver->state().suspended();
        // Allocate from a task-child pool inside the callback so any
        // future hardening that asserts on pool kind sees a realistic
        // workload. The Buffer is released when this scope exits.
        auto leafPool =
            driver->task()->pool()->addLeafChild("bridge-callback-test-pool");
        constexpr int64_t kAllocSize = 8 * 1024;
        void* buffer = leafPool->allocate(kAllocSize);
        leafPool->free(buffer, kAllocSize);
      });

  task->start(2, 1);
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 30'000'000));

  EXPECT_TRUE(callbackFired) << "bridge callback never fired";
  EXPECT_TRUE(sawSuspendedDriver)
      << "callback ran on a driver that was NOT suspended; the suspend "
         "contract around fireHashTableReadyCallback is broken";

  task.reset();
  waitForAllTasksToBeDeleted();
}

// Same as above but with multiple build drivers. Verifies that the
// allPeersFinished gather-then-fire path correctly suspends the last driver
// — the one that survives past the barrier — before invoking the callback.
TEST_F(HashJoinBridgeCallbackTest, callbackSuspendsLastDriverOfManyBuilders) {
  auto vectors = makeProbeAndBuildVectors();

  core::PlanNodeId joinNodeId;
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan =
      PlanBuilder(planNodeIdGenerator)
          .values(vectors, true)
          .hashJoin(
              {"c0"},
              {"u0"},
              PlanBuilder(planNodeIdGenerator)
                  .values(vectors, true)
                  .project({"c0 AS u0", "c1 AS u1"})
                  .planNode(),
              "",
              {"c0", "c1"},
              core::JoinType::kInner)
          .capturePlanNodeId(joinNodeId)
          .planFragment();

  auto queryPool = memory::memoryManager()->addRootPool(
      "callbackSuspendsLastDriverOfManyBuilders",
      1UL << 30,
      exec::MemoryReclaimer::create());
  auto queryCtx = core::QueryCtx::create(
      driverExecutor_.get(),
      core::QueryConfig{{}},
      std::unordered_map<std::string, std::shared_ptr<config::ConfigBase>>{},
      nullptr,
      std::move(queryPool),
      nullptr);

  auto task = Task::create(
      "callback-suspended-last-driver",
      std::move(plan),
      0,
      std::move(queryCtx),
      Task::ExecutionMode::kParallel,
      [](RowVectorPtr /*data*/,
         bool /*drained*/,
         ContinueFuture* /*future*/) { return BlockingReason::kNotBlocked; });

  std::atomic<int> callbackFires{0};
  std::atomic<bool> sawNonSuspendedDriver{false};
  task->registerHashJoinBridgeCallback(
      joinNodeId,
      [&](const BaseHashTable& /*mainTable*/,
          const std::vector<std::unique_ptr<BaseHashTable>>& /*otherTables*/,
          bool /*hasNullKeys*/) {
        ++callbackFires;
        auto* threadCtx = driverThreadContext();
        ASSERT_NE(threadCtx, nullptr);
        auto* driver = threadCtx->driverCtx()->driver;
        if (!driver->state().suspended()) {
          sawNonSuspendedDriver = true;
        }
      });

  task->start(4, 1);
  ASSERT_TRUE(waitForTaskCompletion(task.get(), 30'000'000));

  // The bridge callback is move-cleared on first fire — even with 4 build
  // drivers, exactly one driver (the last to allPeersFinished) invokes it.
  EXPECT_EQ(callbackFires.load(), 1);
  EXPECT_FALSE(sawNonSuspendedDriver.load())
      << "callback fired from a non-suspended driver";

  task.reset();
  waitForAllTasksToBeDeleted();
}

} // namespace
} // namespace facebook::velox::exec::test
