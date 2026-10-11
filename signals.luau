--!strict

export type Connection = {
	Disconnect: (self: Connection) -> (),
	Connected: boolean,
}

type SignalFields<T...> = {
	listeners: { [Connection]: (T...) -> () },
	waitingThreads: { [thread]: boolean },
}

export type Signal<T...> = {
	Connect: (self: Signal<T...>, func: (T...) -> ()) -> Connection,
	Once: (self: Signal<T...>, func: (T...) -> ()) -> Connection,
	Fire: (self: Signal<T...>, T...) -> (),
	Wait: (self: Signal<T...>) -> T...,
	DisconnectAll: (self: Signal<T...>) -> (),
	Destroy: (self: Signal<T...>) -> (),

	_isDestroyed: boolean,
	_fields: SignalFields<T...>,
}

local Signals = {}
Signals.__index = Signals

-- Constructor
function Signals.new<T...>(): Signal<T...>
	local self = setmetatable({}, Signals) :: any

	self._isDestroyed = false
	self._fields = {
		listeners = {},
		waitingThreads = {},
	}

	return self
end

-- API
function Signals:Connect<T...>(func: (T...) -> ()): Connection
	local self = self :: Signal<T...>
	if self._isDestroyed then
		error("Attempt to call :Connect() on a destroyed Signal")
	end

	local connection = { Connected = true } :: Connection

	function connection:Disconnect()
		if not self.Connected then return end
		self.Connected = false

		local parentFields = (self :: any)._parentFields
		if parentFields then
			parentFields.listeners[self] = nil
		end
	end

	(connection :: any)._parentFields = self._fields
	self._fields.listeners[connection] = func

	return connection
end

function Signals:Once<T...>(func: (T...) -> ()): Connection
	local self = self :: Signal<T...>
	local connection: Connection

	connection = self:Connect(function(...: T...)
		connection:Disconnect()
		func(...)
	end)

	return connection
end

function Signals:Fire<T...>(...: T...)
	local self = self :: Signal<T...>
	if self._isDestroyed then return end

	for connection, func in pairs(self._fields.listeners) do
		if connection.Connected then
			task.spawn(func, ...) 
		end
	end
end

function Signals:Wait<T...>(): T...
	local self = self :: Signal<T...>
	if self._isDestroyed then
		error("Attempt to call :Wait() on a destroyed Signal")
	end

	local waitingCoroutine = coroutine.running()
	self._fields.waitingThreads[waitingCoroutine] = true

	local connection: Connection
	connection = self:Connect(function(...: T...)
		connection:Disconnect()
		self._fields.waitingThreads[waitingCoroutine] = nil
		task.spawn(waitingCoroutine, ...)
	end)

	local results = { coroutine.yield() }
	self._fields.waitingThreads[waitingCoroutine] = nil

	local unpack = table.unpack :: any
end

function Signals:DisconnectAll<T...>()
	local self = self :: Signal<T...>

	for connection in pairs(self._fields.listeners) do
		connection:Disconnect()
	end
	table.clear(self._fields.listeners)
end

function Signals:Destroy<T...>()
	local self = self :: Signal<T...>
	if self._isDestroyed then return end
	self._isDestroyed = true

	self:DisconnectAll()

	for waitingCoroutine in pairs(self._fields.waitingThreads) do
		task.spawn(waitingCoroutine, nil)
	end
	table.clear(self._fields.waitingThreads)
end

return Signals
