defprotocol Quantum.Storage do
  alias Quantum.Job
  @doc """
  Inserts the %Job{} struct into storage.
  """
  @callback insert(job :: Job.t()) :: :ok|{:error, reason}
  @doc """
  Updates the job in the storage.
  """
  @callback update(job :: Job.t()) :: :ok|{:error, reason}
  @doc """
  Retrieves the job from the storage by the given name.
  """
  @callback get(name :: atom() | String.t() | reference()) :: Job.t() | nil
  @doc """
  Determines whether the job with a specified name exists in the storage.
  """
  @callback exist?(name :: atom() | String.t() | reference()) :: boolean()
  def exist?(name) do
    case get(name) do
      %Job{} -> true
      nil -> false
    end
  end
  @doc """
  Deletes the job from the storage by the given name.
  """
  @callback delete(name :: atom() | String.t() | reference()) :: :ok | :not_found
  @doc """
  Gets all the jobs from the storage.
  """
  @callback get_all_jobs() :: list(Job.t())
  @doc """
  Saves the job to the storage.
  Inserts the job if it does not exist in the storage, updates it otherwise.
  """
  @callback save(Job.t()) :: :ok | {:error, reason}
  @spec save(Job.t()) :: :ok | {:error, reason}
  def save(job) do
    if exist?(job.name) do
      update(job)
    else
      insert(job)
    end
  end

  defoverridable save: 1, exist?: 1

  @optional_callbacks save: 1, exist?: 1

  @doc """
  Saves a collection of jobs as atomic operation.
  """
  @callback save_all(Enumerable.t()) :: :ok|{:error, reason}
end
