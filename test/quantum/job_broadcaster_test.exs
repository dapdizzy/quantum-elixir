defmodule Quantum.JobBroadcasterTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias Quantum.JobBroadcaster
  alias Quantum.TestConsumer
  alias Quantum.Job
  alias Quantum.Storage.Test, as: TestStorage

  import ExUnit.CaptureLog

  doctest JobBroadcaster

  defmodule TestScheduler do
    @moduledoc false

    use Quantum.Scheduler, otp_app: :job_broadcaster_test
  end

  setup tags do
    if tags[:listen_storage] do
      Process.put(:test_pid, self())
    end

    active_job = TestScheduler.new_job()
    inactive_job = Job.set_state(TestScheduler.new_job(), :inactive)

    init_jobs =
      case tags[:jobs] do
        :both ->
          [active_job, inactive_job]

        :active ->
          [active_job]

        :inactive ->
          [inactive_job]

        _ ->
          []
      end

    broadcaster =
      if tags[:manual_dispatch] do
        nil
      else
        # TODO: Quiet Log Messages

        {:ok, broadcaster} =
          start_supervised({JobBroadcaster, {__MODULE__, init_jobs, TestStorage, TestScheduler}})

        {:ok, _consumer} = start_supervised({TestConsumer, [broadcaster, self()]})

        broadcaster
      end

    {
      :ok,
      %{
        broadcaster: broadcaster,
        active_job: active_job,
        inactive_job: inactive_job
      }
    }
  end

  describe "init" do
    @tag jobs: :both
    test "config jobs", %{active_job: active_job, inactive_job: inactive_job} do
      refute_receive {:received, {:add, ^inactive_job}}
      assert_receive {:received, {:add, ^active_job}}
    end

    @tag manual_dispatch: true
    test "storage jobs", %{active_job: active_job, inactive_job: inactive_job} do
      capture_log(fn ->
        defmodule FullStorage do
          @moduledoc false

          use Quantum.Storage.Test

          def jobs(_),
            do: [
              TestScheduler.new_job(),
              Job.set_state(TestScheduler.new_job(), :inactive)
            ]
        end

        {:ok, broadcaster} =
          start_supervised({JobBroadcaster, {__MODULE__, [], FullStorage, TestScheduler}})

        {:ok, _consumer} = start_supervised({TestConsumer, [broadcaster, self()]})

        assert_receive {:received, {:add, _}}
        refute_receive {:received, {:add, _}}
      end)
    end
  end

  describe "add" do
    @tag listen_storage: true
    test "active", %{broadcaster: broadcaster, active_job: active_job} do
      capture_log(fn ->
        TestScheduler.add_job(broadcaster, active_job)

        assert_receive {:received, {:add, ^active_job}}

        assert_receive {:add_job, {TestScheduler, ^active_job}, _}
      end)
    end

    @tag listen_storage: true
    test "inactive", %{broadcaster: broadcaster, inactive_job: inactive_job} do
      capture_log(fn ->
        TestScheduler.add_job(broadcaster, inactive_job)

        refute_receive {:received, {:add, _}}

        assert_receive {:add_job, {TestScheduler, ^inactive_job}, _}
      end)
    end
  end

  describe "delete" do
    @tag jobs: :active, listen_storage: true
    test "active", %{broadcaster: broadcaster, active_job: active_job} do
      active_job_name = active_job.name

      capture_log(fn ->
        TestScheduler.delete_job(broadcaster, active_job.name)

        assert_receive {:received, {:remove, ^active_job_name}}

        assert_receive {:delete_job, {TestScheduler, ^active_job_name}, _}
      end)
    end

    @tag listen_storage: true
    test "missing", %{broadcaster: broadcaster} do
      capture_log(fn ->
        TestScheduler.delete_job(broadcaster, make_ref())

        refute_receive {:received, {:remove, _}}

        refute_receive {:delete_job, {TestScheduler, _}, _}
      end)
    end

    @tag jobs: :inactive, listen_storage: true
    test "inactive", %{broadcaster: broadcaster, inactive_job: inactive_job} do
      capture_log(fn ->
        inactive_job_name = inactive_job.name

        TestScheduler.delete_job(broadcaster, inactive_job.name)

        refute_receive {:received, {:remove, _}}

        assert_receive {:delete_job, {TestScheduler, ^inactive_job_name}, _}
      end)
    end
  end

  describe "change_state" do
    @tag jobs: :active, listen_storage: true
    test "active => inactive", %{broadcaster: broadcaster, active_job: active_job} do
      active_job_name = active_job.name

      capture_log(fn ->
        TestScheduler.deactivate_job(broadcaster, active_job.name)

        assert_receive {:received, {:remove, ^active_job_name}}

        assert_receive {:update_job_state, {TestScheduler, _, _}, _}
      end)
    end

    @tag jobs: :inactive, listen_storage: true
    test "inactive => active", %{broadcaster: broadcaster, inactive_job: inactive_job} do
      capture_log(fn ->
        TestScheduler.activate_job(broadcaster, inactive_job.name)

        active_job = Job.set_state(inactive_job, :active)

        assert_receive {:received, {:add, ^active_job}}

        assert_receive {:update_job_state, {TestScheduler, _, _}, _}
      end)
    end

    @tag jobs: :active, listen_storage: true
    test "active => active", %{broadcaster: broadcaster, active_job: active_job} do
      # Initial
      assert_receive {:received, {:add, ^active_job}}

      capture_log(fn ->
        TestScheduler.activate_job(broadcaster, active_job.name)

        refute_receive {:received, {:add, ^active_job}}

        refute_receive {:update_job_state, {TestScheduler, _, _}, _}
      end)
    end

    @tag jobs: :inactive, listen_storage: true
    test "inactive => inactive", %{broadcaster: broadcaster, inactive_job: inactive_job} do
      inactive_job_name = inactive_job.name

      capture_log(fn ->
        TestScheduler.deactivate_job(broadcaster, inactive_job.name)

        refute_receive {:received, {:remove, ^inactive_job_name}}

        refute_receive {:update_job_state, {TestScheduler, _, _}, _}
      end)
    end

    @tag listen_storage: true
    test "missing", %{broadcaster: broadcaster} do
      capture_log(fn ->
        TestScheduler.deactivate_job(broadcaster, make_ref())
        TestScheduler.activate_job(broadcaster, make_ref())

        refute_receive {:received, {:remove, _}}
        refute_receive {:received, {:add, _}}
        refute_receive {:update_job_state, {TestScheduler, _, _}, _}
      end)
    end
  end

  describe "delete_all" do
    @tag jobs: :both, listen_storage: true
    test "only active jobs", %{
      broadcaster: broadcaster,
      active_job: active_job,
      inactive_job: inactive_job
    } do
      active_job_name = active_job.name
      inactive_job_name = inactive_job.name

      capture_log(fn ->
        TestScheduler.delete_all_jobs(broadcaster)

        refute_receive {:received, {:remove, ^inactive_job_name}}
        assert_receive {:received, {:remove, ^active_job_name}}

        assert_receive {:purge, TestScheduler, _}
      end)
    end
  end

  describe "jobs" do
    @tag jobs: :both
    test "gets all jobs", %{
      broadcaster: broadcaster,
      active_job: active_job,
      inactive_job: inactive_job
    } do
      active_job_name = active_job.name
      inactive_job_name = inactive_job.name

      assert [{^active_job_name, %Job{}}, {^inactive_job_name, %Job{}}] =
               TestScheduler.jobs(broadcaster)
    end
  end

  @tag jobs: :active
  describe "find_job" do
    test "finds correct one", %{broadcaster: broadcaster, active_job: active_job} do
      active_job_name = active_job.name

      assert active_job == TestScheduler.find_job(broadcaster, active_job_name)
    end
  end
end
