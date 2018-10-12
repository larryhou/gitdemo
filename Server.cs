using System;
using System.IO;
using System.Xml.Serialization;
using UnityEngine;
using UnityEngine.Assertions;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Diagnostics;
using TheNextMoba.Utils;
using ProtoBuf;
using ProtoBuf.Meta;
using Apollo;
using moba_protocol;

namespace TheNextMoba.Network
{
	public delegate void NetworkConnectHandler(ConnectEventType type, ApolloResult result);
	public delegate void NetworkMessageHandler(object message);

	public enum ProtocolType:int
	{
		TCP = 0,UDP = 1
	}

	public enum ConnectEventType:int
	{
		CONNECT = 0, SEND, READ, ERROR, RECONNECT, DISCONNECT
	}

	public class NetworkVars
	{
		public const uint MAX_RETRY_NUM = 3;
	}

	public class MessageObject
	{
		public uint index;
		public ushort command;
		public object message;
	}

	public class ProtocolPackage
	{
		public const uint HEAD_LENGTH = 24;

		public UInt32 length;	// 4B
		public UInt32 uin;		// 4B
		public UInt16 version;	// 2B
		public UInt32 appID;	// 4B
		public UInt16 zoneID;	// 2B
		public UInt16 command;	// 2B
		public UInt16 checksum;	// 2B
		public UInt32 index;	// 4B

		public byte[] message;

        public ProtocolPackage()
        {
            if (!UInt16.TryParse(BuildInfo.appversion, out this.version))
            {
                this.version = 0;
            }
        }

		public string FormattedString(string commandDescription)
		{
			return string.Format ("ProtocolPackage index:{0} command:0x{1:X4}:{9} length:{2} message_length:{8} uin:{3} version:{4} appID:{5} zoneID:{6} checksum:{7}", index, command, length, uin, version, appID, zoneID, checksum, length - HEAD_LENGTH, commandDescription);
		}

		public byte[] EncodePackage(byte[] message)
		{
			length = HEAD_LENGTH + (uint)message.Length;
			byte[] result = new byte[length];
			uint position = 0;

			EncodeUInt32 (length	, result, ref position);
			EncodeUInt32 (uin		, result, ref position);
			EncodeUInt16 (version	, result, ref position);
			EncodeUInt32 (appID		, result, ref position);
			EncodeUInt16 (zoneID	, result, ref position);
			EncodeUInt16 (command	, result, ref position);
			EncodeUInt16 (checksum	, result, ref position);
			EncodeUInt32 (index		, result, ref position);

			Assert.AreEqual (HEAD_LENGTH, position);

			Array.Copy (message, 0, result, position, message.Length);
			return result;
		}

		private void ReadPackageHead(byte[] data)
		{
			using (MemoryStream stream = new MemoryStream (data))
			using (BinaryReader reader = new BinaryReader (stream)) 
			{
				length		= DecodeUInt32 (reader);
				uin			= DecodeUInt32 (reader);
				version		= DecodeUInt16 (reader);
				appID		= DecodeUInt32 (reader);
				zoneID		= DecodeUInt16 (reader);
				command		= DecodeUInt16 (reader);
				checksum	= DecodeUInt16 (reader);
				index 		= DecodeUInt32 (reader);
			}
		}

		private byte[] _buffer;
		private byte[] _remain;
		private uint _bytesReceived;
		private bool _headComplete;
		private bool _bodyComplete;

		public bool ReadConnectionStream(byte[] data)
		{
			if (_buffer == null)
			{
				_buffer = new byte[HEAD_LENGTH];
			}

			uint position = _bytesReceived;

			_bytesReceived += (uint)data.Length;

			if (!_headComplete)
			{
				if (_bytesReceived >= HEAD_LENGTH) 
				{
					byte[] bytes = new byte[HEAD_LENGTH];
					Array.Copy (_buffer, bytes, position);
					Array.Copy (data, 0, bytes, position, HEAD_LENGTH - position);

					ReadPackageHead (bytes);
					_headComplete = true;

					Array.Clear (bytes, 0, bytes.Length);
				} 
				else 
				{
					Array.Copy (data, 0, _buffer, position, data.Length);
				}
			}

			if (_headComplete)
			{
				if (_buffer.Length == HEAD_LENGTH)
				{
					byte[] buffer = new byte[length];
					Array.Copy (_buffer, buffer, _buffer.Length);
					Array.Clear (_buffer, 0, _buffer.Length);

					_buffer = buffer;
				}

				Array.Copy (data, 0, _buffer, position, Math.Min (data.Length, length - position));

				if (_bytesReceived >= length) 
				{
					_bodyComplete = true;

					message = new byte[length - HEAD_LENGTH];
					Array.Copy (_buffer, HEAD_LENGTH, message, 0, message.Length);

					if (_bytesReceived > length)
					{
						_remain = new byte[_bytesReceived - length];
						Array.Copy (data, length - position, _remain, 0, _remain.Length);
					}
				}
			}

			Array.Clear (data, 0, data.Length);
			return _headComplete && _bodyComplete;
		}

		public bool HeadComplete{ get { return _headComplete; } }
		public bool BodyComplete{ get { return _bodyComplete; } }

		public byte[] StripRemainByteArray()
		{
			if (_headComplete && _bodyComplete) 
			{
				if (_remain != null)
				{
					return _remain.Clone() as byte[];
				}
			}

			return null;
		}

		public byte[] StripPackageByteArray()
		{
			return _buffer;
		}

		public void Clear()
		{
			if (_remain != null) 
			{
				Array.Clear (_remain, 0, _remain.Length);
				_remain = null;
			}

			if (_buffer != null) 
			{
				Array.Clear (_buffer, 0, _buffer.Length);
				_buffer = null;
			}

			if (message != null) 
			{
				Array.Clear (message, 0, message.Length);
				message = null;
			}

			_bytesReceived = 0;
			_headComplete = false;
			_bodyComplete = false;
		}

		UInt16 DecodeUInt16(BinaryReader reader)
		{
			return (UInt16)System.Net.IPAddress.NetworkToHostOrder (reader.ReadInt16 ());
		}

		UInt32 DecodeUInt32(BinaryReader reader)
		{
			return (UInt32)System.Net.IPAddress.NetworkToHostOrder (reader.ReadInt32 ());
		}

		void EncodeUInt16(UInt16 num, byte[] result, ref uint position)
		{
			UInt16 value = (UInt16)System.Net.IPAddress.HostToNetworkOrder ((Int16)num);
			byte[] bytes = BitConverter.GetBytes (value);

			Array.Copy (bytes, 0, result, position, bytes.Length);
			position += (uint)bytes.Length;
		}

		void EncodeUInt32(UInt32 num, byte[] result, ref uint position)
		{
			UInt32 value = (UInt32)System.Net.IPAddress.HostToNetworkOrder ((Int32)num);
			byte[] bytes = BitConverter.GetBytes (value);

			Array.Copy (bytes, 0, result, position, bytes.Length);
			position += (uint)bytes.Length;
		}
	}

	public class Server<Y,E> : MonoBehaviour where Y : Server<Y,E>
	{	
		private IApolloConnector _connector;
		protected string _dhp = "C0FC17D2ADC0007C512E9B6187823F559595D953C82D3D4F281D5198E86C79DF14FAB1F2A901F909FECB71B147DBD265837A254B204D1B5BC5FD64BF804DCD03";

		private NetworkConnectHandler _connectHandler;
		protected ProtocolPackage _protocol;
		protected ProtocolType _type;
		private uint _sequence;

		private Dictionary<ushort, Type> _commandRegisterMap = new Dictionary<ushort, Type>();
		private Dictionary<ushort, NetworkMessageHandler> _messageHandlers = new Dictionary<ushort, NetworkMessageHandler>();
		private Dictionary<uint, NetworkMessageHandler> _indexdMessageHandlers = new Dictionary<uint, NetworkMessageHandler>();
		private Dictionary<ushort, bool> _logIgnoreMap = new Dictionary<ushort, bool>();
		private List<MessageObject> _messages = new List<MessageObject>();

		private Dictionary<ushort, E> _commandEnumMap;

		/// <summary>
		/// UDP方式连接时有效，true表示可靠UDP方式传输数据，否则可丢包
		/// Gets or sets a value indicating whether this <see cref="TheNextMoba.Network.Server`2"/> is reliable.
		/// </summary>
		/// <value><c>true</c> if reliable; otherwise, <c>false</c>.</value>
		public bool reliable { get; set;}
		public uint uin { get; set;}

		void Awake()
		{
			ADebug.Level = ADebug.LogPriority.None;
			ApolloInfo info = new ApolloInfo (102400);
			IApollo.Instance.Initialize (info);
			IApollo.Instance.SetLogPriority(ApolloLogPriority.Debug);

			InitCommandEnumMap ();
		}

		private void InitCommandEnumMap()
		{
			_commandEnumMap = new Dictionary<ushort, E> ();

			Type genericType = typeof(E);
			if (genericType.IsEnum)
			{
				foreach(E value in Enum.GetValues (genericType))
				{
					ushort key = (ushort)value.GetHashCode ();
					_commandEnumMap.Add (key, value);
				}
			}
		}

		public E FindEnumValueByCommand(ushort command)
		{
			if (_commandEnumMap.ContainsKey (command))
			{
				return _commandEnumMap [command];
			}

			return default(E);
		}

		public string FormatCommandToEnumString(ushort command)
		{
			E commandEnumValue = FindEnumValueByCommand (command);
			return string.Format ("{0}.{1}", commandEnumValue.GetType (), commandEnumValue);
		}

		//MARK: set command ignore flag
		public void IgnoreCommandLog(ushort command)
		{
			_logIgnoreMap [command] = true;
		}

		public void IgnoreCommandLog(ushort command, bool ignore)
		{
			if (ignore)
			{
				_logIgnoreMap.Remove (command);
			} 
			else
			{
				_logIgnoreMap [command] = true;
			}
		}

		private bool IsCommandVerbose(ushort command)
		{
			bool ignore;
			_logIgnoreMap.TryGetValue (command, out ignore);

			return ignore == false;
		}

		//MARK: Singleton Implements
		protected static Y _insance;
		public static Y Instance
		{
			get 
			{
				if (_insance == null)
				{
					GameObject go = new GameObject(typeof(Y).ToString());
					DontDestroyOnLoad(go);

					_insance = go.AddComponent<Y>();
				}

				return _insance;
			}
		}

		//MARK: Manage Command Registers
		public void RegisterCommandType(ushort command, Type type)
		{
			if (type == null)
			{
				return;
			}

			//Log("RegisterCommandType : " + FormatCommandToEnumString(command) + " type : " + type);
			if (!_commandRegisterMap.ContainsKey (command)) 
			{
				_commandRegisterMap.Add (command, type);
			}
			else 
			{
				_commandRegisterMap [command] = type;
			}
		}


		public void UnregisterCommandType(ushort command)
		{
//			Log("UnregisterCommandType : " + FormatCommandToEnumString(command));
			if (_commandRegisterMap.ContainsKey (command)) 
			{
				_commandRegisterMap.Remove (command);
			}
		}

		public Type GetTypeByCommand(ushort command)
		{
			if (_commandRegisterMap.ContainsKey(command))
			{
				return _commandRegisterMap [command];
			}

			return null;
		}

		private Type GetTypeByCommand<T>(ushort command, T message) where T : ProtoBuf.IExtensible
		{
			if (_commandRegisterMap.ContainsKey(command))
			{
				return _commandRegisterMap [command];
			}

			Type reqType = message.GetType ();

			string typeName = Regex.Replace (reqType.ToString (), @"Req$", "Rsp");
			Type rspType = Type.GetType (typeName);

			_commandRegisterMap [command] = rspType;

			return rspType;
		}

		//MARK: Manage Message Handlers
		public void AddMessageHandler(ushort command, NetworkMessageHandler handler)
		{
			if (!_messageHandlers.ContainsKey (command)) 
			{
				_messageHandlers.Add (command, null);
			}

			_messageHandlers [command] += handler;
		}

		public void RemoveMessageHandler(ushort command, NetworkMessageHandler handler)
		{
			if (_messageHandlers.ContainsKey (command))
			{
				_messageHandlers [command] -= handler;
			}
		}

		private void AddIndexedMessageHandler(uint index, NetworkMessageHandler handler)
		{
			if (!_indexdMessageHandlers.ContainsKey (index))
			{
				_indexdMessageHandlers.Add (index, null);
			}

			_indexdMessageHandlers [index] += handler;
		}

		private void RemoveIndexedMessageHandler(uint index)
		{
			if (_indexdMessageHandlers.ContainsKey (index))
			{
				_indexdMessageHandlers.Remove (index);
			}
		}

		//MARK: Trigger Message Handlers
		private void TriggerHandlersWithMessage(ushort command, object message)
		{
			lock (_messageHandlers)
			{
				NetworkMessageHandler handler;
				if (_messageHandlers.TryGetValue (command, out handler))
				{
					if (handler != null)
					{
					//	Log ("handler for " + FormatCommandToEnumString (command));
						handler (message);
					} 
					else
					{
						Log ("no handler for " + FormatCommandToEnumString (command));
					}
				}
			}
		}

		private void TriggerHandlersWithMessage(uint index, object message)
		{
			lock (_indexdMessageHandlers)
			{
				NetworkMessageHandler handler;
				if (_indexdMessageHandlers.TryGetValue (index, out handler))
				{
					if (handler != null)
					{
						handler (message);
					}
				}
			}
		}

		//MARK: Manage Connect Event Listeners
		public void AddConnectHandler(NetworkConnectHandler handler)
		{
			_connectHandler += handler;
		}

		public void RemoveConnectHandler(NetworkConnectHandler handler)
		{
			_connectHandler -= handler;
		}

		//MARK: Connect Operations
		virtual public void Connect(string ip, uint port, ProtocolType type, string dhp = null)
		{
			_type = type;
			Log (string.Format ("Connect ProtocolType.{0} ip:{1} port:{2} dhp:{3}", type, ip, port, dhp));

			_sequence = 1;
			_protocol = new ProtocolPackage ();

			if(dhp == null)
			{
				dhp = _dhp;
			}
			else
			{
				_dhp = dhp;
			}
			
			if (type == ProtocolType.UDP) 
			{
				_connector = IApollo.Instance.CreateApolloConnection (ApolloPlatform.None, "lwip://" + ip + ":" + port);
			} 
			else 
			{
				_connector = IApollo.Instance.CreateApolloConnection (ApolloPlatform.None, "tcp://" + ip + ":" + port);
			}

			_connector.ConnectEvent += new ConnectEventHandler (ApolloConnectHandler);
			_connector.RecvedDataEvent += new RecvedDataHandler(ApolloRecievedDataEventHandler);
			_connector.ErrorEvent += new ConnectorErrorEventHandler (ApolloErrorHandler);
			_connector.DisconnectEvent += new DisconnectEventHandler(ApolloDisconnectHandler);
			_connector.ReconnectEvent += new ReconnectEventHandler (ApolloReconnectHandler);

			_connector.SetSecurityInfo (ApolloEncryptMethod.None, ApolloKeyMaking.None, dhp);
			ApolloResult r = _connector.Connect ();
			Log (string.Format ("Connect... ApolloResult.{0}", r));
		}

		public void RegisterCommand(ushort command, Type responseMessageType, NetworkMessageHandler handler)
		{
			RegisterCommandType (command, responseMessageType);
			AddMessageHandler (command, handler);
		}

		public void UnregisterCommand(ushort command, NetworkMessageHandler handler)
		{
			UnregisterCommandType (command);
			RemoveMessageHandler (command, handler);
		}

		/// <summary>
		/// All In One.
		/// Send the specified command, message and handler.
		/// </summary>
		/// <param name="command">Command.</param>
		/// <param name="message">Message.</param>
		/// <param name="handler">Handler.</param>
		/// <typeparam name="T">The 1st type parameter.</typeparam>
		public void Send<T>(ushort command, T message, NetworkMessageHandler handler) where T: ProtoBuf.IExtensible
		{
			RegisterCommandType (command, GetTypeByCommand (command, message));
			AddIndexedMessageHandler (_sequence, handler);

			Send<T> (command, message);
		}

		public void SendSyncRequest(byte[] data){
			if (!Connected) 
			{
				ViewDebug.LogError ("Client's not connected!");
				return;
			}
			// Setup Protocol Head
			ProtocolPackage protocol = new ProtocolPackage();
			protocol.command =(ushort) GameSvrCmd.GAME_LOGIC_PGK_REQ;
			protocol.uin = uin;
			protocol.index = _sequence++;
			byte[] bytes = protocol.EncodePackage (data);
			ApolloResult result;
			if (_type == ProtocolType.TCP || reliable) 
			{
				result = _connector.WriteData (bytes);
			} 
			else 
			{
				result = _connector.WriteUdpData (bytes);
			}
			DispatchConnectEvent (ConnectEventType.SEND, result);
		}

		public int up_data_size = 0;
		public int down_data_size = 0;
		public int data_size{get{ return up_data_size + down_data_size;}}

		public void Send<T>(ushort command, T message) where T: ProtoBuf.IExtensible
		{
			if (!Connected) 
			{
				ViewDebug.LogError ("Client's not connected!");
				return;
			}
			
			// Setup Protocol Head
			ProtocolPackage protocol = new ProtocolPackage();
			protocol.command = command;
			protocol.uin = uin;
			protocol.index = _sequence++;

			// Serialize Message
			MemoryStream stream = new MemoryStream ();
			ServerUtils.ProtocolSerializer.Serialize(stream, message);

			
			byte[] data = protocol.EncodePackage (stream.ToArray());
			up_data_size += data.Length;
			// Log Sent Message
			if(IsCommandVerbose(command))
			{
				Log (string.Format("send >> {0} \n {1}", protocol.FormattedString (FormatCommandToEnumString(command)), message.FormattedString ()));
			}

			ApolloResult result;
			if (_type == ProtocolType.TCP || reliable) 
			{
				result = _connector.WriteData (data);
			} 
			else 
			{
				result = _connector.WriteUdpData (data);
			}

			DispatchConnectEvent (ConnectEventType.SEND, result);
		}

		virtual public void Reconnect()
		{
			_connector.Reconnect ();
		}

		public void Close()
		{
			if (_connector != null) 
			{
				_connector.ConnectEvent -= new ConnectEventHandler (ApolloConnectHandler);
				_connector.RecvedDataEvent -= new RecvedDataHandler(ApolloRecievedDataEventHandler);
				_connector.ErrorEvent -= new ConnectorErrorEventHandler (ApolloErrorHandler);
				_connector.DisconnectEvent -= new DisconnectEventHandler(ApolloDisconnectHandler);
				_connector.ReconnectEvent -= new ReconnectEventHandler (ApolloReconnectHandler);
				if (_connector.Connected) 
				{
					_connector.Disconnect ();
				}
				IApollo.Instance.DestroyApolloConnector(_connector);
				_connector = null;
			}

			if (_protocol != null) 
			{
				_protocol.Clear ();
				_protocol = null;
			}
		}

		public bool Connected
		{
			get { return _connector != null && _connector.Connected; }
		}

		//MARK: Parsing Data Stream
		private void ApolloRecievedDataEventHandler()
		{
			ApolloResult result = ApolloResult.Success;

			while (result == ApolloResult.Success) 
			{
				byte[] buffer;
				if (_type == ProtocolType.TCP || reliable) 
				{
					result = _connector.ReadData (out buffer);
				}
				else 
				{
					result = _connector.ReadUdpData (out buffer);
				}

				if (result == ApolloResult.Success) 
				{
					down_data_size += buffer.Length;
					ReadConnectionStream (buffer);
				}

				DispatchConnectEvent (ConnectEventType.READ, result);
			}
		}

		private void ReadConnectionStream(byte[] buffer)
		{
			if (_protocol.ReadConnectionStream(buffer))
			{
				byte[] remain = _protocol.StripRemainByteArray ();

				Type type = GetTypeByCommand(_protocol.command);

				object message;
				if (type != null)
				{
					// Deserialize Message
					MemoryStream stream = new MemoryStream (_protocol.message);
					message = ServerUtils.ProtocolSerializer.Deserialize(stream, null, type);
//					message = Serializer.NonGeneric.Deserialize (type, stream);

					if (IsCommandVerbose (_protocol.command))
					{
						Log (string.Format("read << {0} type:{1}\n{2}", _protocol.FormattedString(FormatCommandToEnumString(_protocol.command)), type, (message as ProtoBuf.IExtensible).FormattedString()));
					}
//					Log (string.Format ("raw_bytes: \n {0}", _protocol.message.ToHexString ()));
				} 
				else
				{
					// Extract Message Raw Bytes
					if (IsCommandVerbose (_protocol.command))
					{
						Log (string.Format("read << {0} type:RAW_BYTES\n{1}", _protocol.FormattedString(FormatCommandToEnumString(_protocol.command)), _protocol.message.ToHexString()));
					}
					message = _protocol.message.Clone ();
				}

				StackRecievedMessage(_protocol.index, _protocol.command, message);

				_protocol.Clear ();
				if (remain != null)
				{
					ReadConnectionStream (remain);
				}
			}
		}

		public static int _ping;

		private void StackRecievedMessage(uint index, ushort command, object message)
		{
			lock(_messages)
			{
				var msgObj = new MessageObject();
				msgObj.index = _protocol.index;
				msgObj.command = _protocol.command;
				msgObj.message = message;

				if (command ==(short) GameSvrCmd.GAME_PING_RSP) {
					GamePingPkg pkg = message as GamePingPkg;
					_ping = (int)((ulong)(System.DateTime.Now.Ticks / 10000) - pkg.curTime);
				}

				_messages.Add(msgObj);
			}

			FixedUpdate22 ();
		}

		private void FixedUpdate22()
		{
			if(_messages == null || _messages.Count == 0)
				return;
			
			lock(_messages)
			{
				for(int i = 0; i < _messages.Count; i++)
				{
					var msgObj = _messages[i];
					if (msgObj.index > 0 && _indexdMessageHandlers.ContainsKey (msgObj.index))
					{
						TriggerHandlersWithMessage (msgObj.index, msgObj.message);
						RemoveIndexedMessageHandler (msgObj.index);
					}
					
					TriggerHandlersWithMessage(msgObj.command, msgObj.message);
				}
				
				_messages.Clear();
			}
		}

		//MARK: Connection Events Handle
		virtual protected void ApolloConnectHandler(ApolloResult result, ApolloLoginInfo loginInfo)
		{
			DispatchConnectEvent (ConnectEventType.CONNECT, result);
			ViewDebug.Log (loginInfo.FormattedString ());
		}

		private void ApolloErrorHandler(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.ERROR, result);
		}

		private void ApolloDisconnectHandler(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.DISCONNECT, result);
		}

		private void ApolloReconnectHandler(ApolloResult result)
		{
			DispatchConnectEvent (ConnectEventType.RECONNECT, result);
		}

		protected void DispatchConnectEvent(ConnectEventType type, ApolloResult result)
		{
			if (type != ConnectEventType.READ && type != ConnectEventType.SEND)
			{
				Log (string.Format ("ConnectEventType.{0} ApolloResult.{1}", type, result));
			}

			if (_connectHandler != null) 
			{
				_connectHandler (type, result);
			}
		}

		[Conditional("DEBUG")]
		private void Log(string message)
		{
			ViewDebug.Log (string.Format ("[{0:HH:mm:ss.fff} {1}] {2}", DateTime.Now, typeof(Y), message));
		}
	}
}

