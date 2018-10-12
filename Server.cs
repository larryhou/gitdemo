using System;
using System.IO;
using UnityEngine;
using UnityEngine.Assertions;
using System.Collections;
using System.Collections.Generic;
using TheNextMoba.Utils;
using ProtoBuf;
using Apollo;

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

		override public string ToString()
		{
			return string.Format ("ProtocolPackage index:{0} command:0x{1:X}/{1} length:{2} uin:{3} version:{4} appID:{5} zoneID:{6} checksum:{7}", index, command, length, uin, version, appID, zoneID, checksum);
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

			Assert.AreEqual (HEAD_LENGTH, (uint)result.Length);
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

		public byte[] StripRemainBytes()
		{
			if (_headComplete && _bodyComplete) 
			{
				return _remain.Clone() as byte[];
			}

			return null;
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

	public class Server//<Y> where Y : Server<Y>, new()
	{	
		private IApolloConnector _connector;
		private string _dhp = "C0FC17D2ADC0007C512E9B6187823F559595D953C82D3D4F281D5198E86C79DF14FAB1F2A901F909FECB71B147DBD265837A254B204D1B5BC5FD64BF804DCD03";

		private NetworkConnectHandler _connectHandler;
		private ProtocolPackage _protocol;
		private ProtocolType _type;
		private uint _sequence;

		private Dictionary<ushort, Type> _commandRegisterMap = new Dictionary<ushort, Type>();
		private Dictionary<ushort, NetworkMessageHandler> _messageHandles = new Dictionary<ushort, NetworkMessageHandler>();

		private List<MessageObject> _messages = new List<MessageObject>();

		public uint uin { get; set;}

		public Server ()
		{
			ApolloInfo info = new ApolloInfo (102400);
			IApollo.Instance.Initialize (info);
		}

		//MARK: Singleton Implements
//		protected static Y _insance;
//		public static Y Instance
//		{
//			get 
//			{
//				if (_insance == null)
//				{
//					_insance = new Y ();
//				}
//
//				return _insance;
//			}
//		}

		//MARK: Manage Command Registers
		public void RegisterCommandType(ushort command, Type type)
		{
			Debug.Log("RegisterCommandType : " + command + " type : " + type);
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
			Debug.Log("UnregisterCommandType : " + command);
			if (_commandRegisterMap.ContainsKey (command)) 
			{
				_commandRegisterMap.Remove (command);
			}
		}

		public Type GetTypeByCommand(ushort command)
		{
			Debug.Log("GetTypeByCommand : " + command);
			if (_commandRegisterMap.ContainsKey(command))
			{
				return _commandRegisterMap [command];
			}

			return null;
		}

		//MARK: Manage Message Handlers
		public void AddMessageHandler(ushort command, NetworkMessageHandler handler)
		{
			if (!_messageHandles.ContainsKey (command)) 
			{
				_messageHandles.Add (command, handler);
			}
			else 
			{
				_messageHandles [command] += handler;
			}
		}

		public void RemoveMessageHandler(ushort command, NetworkMessageHandler handler)
		{
			if (_messageHandles.ContainsKey (command)) 
			{
				_messageHandles [command] -= handler;
			}
		}

		//MARK: Trigger Message Handlers
		private void TriggerHandlesWithMessage(ushort command, object message)
		{
			lock (_messages)
			{
				MessageObject msg = new MessageObject ();
				msg.command = command;
				msg.message = message;
				_messages.Add(msg);
			}
		}

		public void Update()
		{
			lock (_messages) 
			{
				if (_messages.Count > 0) 
				{
					for (int i = 0; i < _messages.Count; i++) 
					{
						MessageObject msg = _messages [i];

						NetworkMessageHandler handle;
						if (_messageHandles.TryGetValue (msg.command, out handle)) 
						{
							try
							{
								if (handle != null)
								{
									handle(msg.message);
								}
							}
							catch (Exception e)
							{
								Debug.LogWarning("TriggerHandlesWithMessage exception : " + e.Message + "  stackTrace : " + e.StackTrace);
							}
						}
					}

					_messages.Clear ();
				}
			}
		}

		//MARK: Manage Connect Event Listeners
		public void AddConnectHandler(NetworkConnectHandler handler)
		{
			_connectHandler -= handler;
			_connectHandler += handler;
		}

		public void RemoveConnectHandler(NetworkConnectHandler handler)
		{
			_connectHandler -= handler;
		}

		public bool Connected
		{
			get { return _connector != null && _connector.Connected; }
		}

		private void DispatchConnectEvent(ConnectEventType type, ApolloResult result)
		{
			Debug.Log (string.Format ("ConnectEventType.{0} : ApolloResult.{1}", type, result));

			if (_connectHandler != null) 
			{
				_connectHandler (type, result);
			}
		}

		//MARK: Parsing Data Stream
		private void ApolloRecievedDataEventHandler()
		{
			ApolloResult result = ApolloResult.Success;

			while (result == ApolloResult.Success) 
			{
				byte[] buffer;
				if (_type == ProtocolType.TCP) 
				{
					result = _connector.ReadData (out buffer);
				}
				else 
				{
					result = _connector.ReadUdpData (out buffer);
				}

				if (result == ApolloResult.Success) 
				{
					ReadConnectionStream (buffer);
				}

				DispatchConnectEvent (ConnectEventType.READ, result);
			}
		}

		private void ReadConnectionStream(byte[] buffer)
		{
			if (_protocol.ReadConnectionStream(buffer))
			{
				byte[] remain = _protocol.StripRemainBytes ();

				Type type = GetTypeByCommand(_protocol.command);

				object message;
				if (type != null)
				{
					// Deserialize Message
					Debug.Log ("[RSP-BODY]command : " + _protocol.command + " message_length : " + _protocol.message.Length + " type : " + type);
					MemoryStream stream = new MemoryStream (_protocol.message);
					message = Serializer.NonGeneric.Deserialize (type, stream);
				} 
				else
				{
					// Extract Message Raw Bytes
					Debug.Log ("[RSP-BODY]command : " + _protocol.command + " message_length : " + _protocol.message.Length + " type : RAW_BYTES");
					message = _protocol.message.Clone ();
				}

				TriggerHandlesWithMessage (_protocol.command, message);

				_protocol.Clear ();
				if (remain != null)
				{
					ReadConnectionStream (remain);
				}
			}
			else if (_protocol.HeadComplete)
			{
				Debug.Log ("[RSP-HEAD]" + _protocol.ToString());
			}
		}

		//MARK: Connection Events Handle
		private void ApolloConnectHandler(ApolloResult result, ApolloLoginInfo loginInfo)
		{
			DispatchConnectEvent (ConnectEventType.CONNECT, result);
			Debug.Log (loginInfo);
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

		//MARK: Connect Operations
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
				_connector = null;
			}

			if (_protocol != null) 
			{
				_protocol.Clear ();
				_protocol = null;
			}
		}

		public void Connect(string ip, int port, ProtocolType type, string dhp = null)
		{
			_type = type;
			Debug.Log (string.Format ("Connect ProtocolType.{0} ip:{1} port:{2} dhp:{3}", type, ip, port, dhp));

			_sequence = 0;
			_protocol = new ProtocolPackage ();

			if (dhp == null)
				dhp = _dhp;
			
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

			_connector.SetSecurityInfo (ApolloEncryptMethod.Aes, ApolloKeyMaking.RawDH, dhp);
			ApolloResult r = _connector.Connect ();
			Debug.Log (r);
		}

		public void Send<T>(ushort command, T message) where T: ProtoBuf.IExtensible
		{
			if (!Connected) 
			{
				Debug.LogError("Client's not connected!");
				return;
			}
			
			// Setup Protocol Head
			ProtocolPackage protocol = new ProtocolPackage();
			protocol.command = command;
			protocol.uin = uin;
			protocol.index = ++_sequence;

			// Serialize Message
			MemoryStream stream = new MemoryStream ();
			Serializer.Serialize<T> (stream, message);

			byte[] data = protocol.EncodePackage (stream.GetBuffer ());

			ApolloResult result;
			if (_type == ProtocolType.TCP) 
			{
				result = _connector.WriteData (data);
			} 
			else 
			{
				result = _connector.WriteUdpData (data);
			}

			DispatchConnectEvent (ConnectEventType.SEND, result);
		}

		public void Reconnect()
		{
			_connector.Reconnect ();
		}

		void OnDestroy()
		{
			Close ();
		}
	}
}

