using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using Infineon.MCS.ScriptEditor.Common;
using Infineon.MCS.ScriptEditor.DataProvider.Logger;
using Infineon.MCS.ScriptEditor.DataProvider.SourceTree;
using Infineon.MCS.ScriptEditor.Dto;
using Infineon.MCS.ScriptEditor.Framework;

namespace Infineon.MCS.ScriptEditor.DataProvider.Debugger
{
    /// <summary>
    /// 1. This class is responsible for sending out commands and recieve response
    /// 2. GDB protocol is used
    /// 3. Response is interpreted and event is raised for every thread stop status;
    /// 4. [MUST DO]Every response received from target should be Acknowledged
    /// </summary>
    public class DebuggerService : IDebuggerService
    {
        #region consts
        const int NumOfTries = 3;
        const string StopDebugCommand = "$D;4#B3";
        const string GetTaskStatusCommand = "$m2000001C,4#93";
        const string SetBreakPointCommand = "Z1,";
        const string RemoveBreakPointCommand = "z1,";
        const string ContinueAllCommand = "$vCont;c:-1#40";
        const string ContinueTask0Command = "$vCont;c:1#13";
        const string ContinueTask1Command = "$vCont;c:2#14";
        const string StopAllTasksCommand = "$vCont;t:-1#51";
        const string StopTask0Command = "$vCont;t:1#EA";
        const string StopTask1Command = "$vCont;t:2#25";
        const string StepTask0Command = "$vCont;s:1#23";
        const string StepTask1Command = "$vCont;s:2#24";
        const string WriteMemoryCommand = "M";
        private const string ResetCommand = "$?#3F";
        private const string ThreadResponseStartTxt = "$T02thread";
        private const string GetCodeCRCCommand = "$m20000000,4#7F";
        private const int WaitTimeInMs = 50;
        private const int ListenerWaitTimeInMs = 100;
        private const int WorkerWaitTimeInMs = 150;
        private const int WaitIfReadMemoryInProgressCount = 500;
        #endregion

        #region private variables
        ILogger logger;
        Thread workerThread;
        Thread listenerThread;
        // Que for debugger commands from the user
        ConcurrentQueue<string> QCommands = new ConcurrentQueue<string>();
        // interpreter for command responses
        PacketInterpreter interpreter = new PacketInterpreter();
        IScriptProject project;
        IWatchVarManager watchVarManager;

        //synchronization flags
        bool readMemoryFlag = false;
        bool isCmdInProgress = false;
        bool isReadMemoryInProgress = false;
        #endregion

        #region static
        static private Semaphore semaphore = new Semaphore(1, 1);
        #endregion

        #region constructors
        public DebuggerService()
        {
            logger = IOC.container.Resolve<ILogger>();
            watchVarManager = IOC.container.Resolve<IWatchVarManager>();
            SocketComn.HaltStatus += HaltStatusEncountered;
        }
        #endregion

        #region public variables
        public bool IsDebugMode { get; private set; } = false;
        // event to raise in case of thread-stop status
        public event Action<List<int>> HaltStatus = delegate { };
        public event Action<WatchVar> WatchVarUpdated = delegate { };
        public bool IsBreakPointHitTask0 { get; set; }
        public bool IsBreakPointHitTask1 { get; set; }
        #endregion

        #region public methods


        /// <summary>
        ///  this method connects to the port and sets up back-ground threads to send commands and receive response
        /// </summary>
        /// <param name="prj"></param>
        /// <param name="portNum"></param>
        /// <returns>bool</returns>
        public bool StartDebug(IScriptProject prj, string portNum)
        {
            this.project = prj;
            if (SocketComn.Connect(portNum))
            {
                IsDebugMode = true;
                workerThread = new Thread(Worker)
                {
                    IsBackground = true
                };
                workerThread.Start();
                listenerThread = new Thread(Listener)
                {
                    IsBackground = true
                };
                listenerThread.Start();
                return true;
            }
            IsDebugMode = false;
            return false;
        }

        /// <summary>
        /// this method disconnects the socket and resets the status flags
        /// </summary>
        public void StopDebug()
        {
            readMemoryFlag = false;
            IsDebugMode = false;
            SendCommand(StopDebugCommand, 1);
            IsBreakPointHitTask0 = IsBreakPointHitTask0 = false;
            SocketComn.DisConnect();
        }

        /// <summary>
        /// this method sends out reset command and waits for halt/thread stop -status
        /// </summary>
        /// <returns>bool</returns>
        /// <exception cref="NoResponseException"></exception>
        public bool Reset()
        {
            int count = 0;
            try
            {
                isCmdInProgress = true;
                semaphore.WaitOne();
                while (count < NumOfTries)
                {
                    SocketComn.ResetMode = true;
                    QCommands.Enqueue(ResetCommand);
                    var mesg = WaitOnQMesg(1);

                    if (mesg != null)
                    {
                        if (mesg.Contains(ThreadResponseStartTxt))
                        {
                            // raises the event if stop-status encountered
                            CheckForStopStatus(mesg, "$T02");
                            return true;
                        }
                        else if (mesg == "-\0")
                        {
                            //TODO: waiting on Firmware programmer 
                        }
                    }
                    count++;
                    Thread.Sleep(WaitTimeInMs);
                    Acknowledge("Reset [Timeout] - Try[" + count + "]");
                }
            }
            finally
            {
                isCmdInProgress = false;
                SocketComn.ResetMode = false;
                semaphore.Release();
            }

            throw new NoResponseException();
        }
        /// <summary>
        /// status provides Task0 and Task1 exists or not, 
        /// based on this, taskx UI buttons are enabled/disabled
        /// </summary>
        /// <returns>int</returns>
        public int? GetTaskxStatus()
        {
            semaphore.WaitOne();
            isCmdInProgress = true;
            int count = 0;
            try
            {
                while (count < NumOfTries)
                {
                    QCommands.Enqueue(GetTaskStatusCommand);
                    var mesg = WaitOnQMesg(1);
                    if (mesg != null)
                    {
                        if (mesg == "-\0")
                        {
                            //TODO: waiting on firmware programmer
                        }
                        else
                        {
                            var taskXstatus = interpreter.GetMemoryValue(mesg, VarDataType.int32_t);
                            Acknowledge("Acknowledge for GetTaskxStatus  " + GetTaskStatusCommand);
                            return taskXstatus;

                        }
                    }
                    count++;
                    Thread.Sleep(WaitTimeInMs);
                    Acknowledge("Reset [Timeout] - Try[" + count + "]");
                }
            }
            finally
            {
                isCmdInProgress = false;
                semaphore.Release();
            }
            throw new NoResponseException();
        }
        /// <summary>
        /// Get CRC code from the downloaded program 
        /// this code compared it with program being debugged
        /// </summary>
        /// <returns></returns>
        public bool CodeCRCcheck()
        {
            string crc = Translator.GetCodeCRC();
            semaphore.WaitOne();
            isCmdInProgress = true;
            string cmd = GetCodeCRCCommand;
            int count = 0;
            try
            {
                while (count < NumOfTries)
                {
                    QCommands.Enqueue(cmd);
                    var mesg = WaitOnQMesg(1);
                    if (mesg != null)
                    {
                        if (mesg == "-\0")
                        {
                            //TODO: waiting on firmware programmer
                        }
                        else
                        {
                            var crcTarget = interpreter.GetV(mesg, 4);
                            Acknowledge("CodeCRCcheck  " + cmd);
                            Log("CRC Check " + crc + " -- " + crcTarget);
                            return string.Compare(crc, crcTarget, true) == 0;
                        }
                    }
                    count++;
                    Thread.Sleep(WaitTimeInMs);
                    Acknowledge("Reset [Timeout] - Try[" + count + "]");
                }
            }
            finally
            {
                isCmdInProgress = false;

                semaphore.Release();

            }
            throw new NoResponseException();
        }

        /// <summary>
        /// Set Break point
        /// </summary>
        /// <param name="bytePos">byte position where break point to be set</param>
        /// <param name="threadId">Id of the thread (1 or 2)</param>
        /// <returns>bool</returns>
        public bool SetBreakPoint(int bytePos, int threadId)
        {
            string cmd = SetBreakPointCommand + bytePos.ToString("X") + "," + threadId.ToString();
            string checksum = CalcCheckSum(cmd);
            cmd = "$" + cmd + "#" + checksum;
            return SendCommand(cmd);
        }

        /// <summary>
        /// Remove Break point
        /// </summary>
        /// <param name="bytePos">byte position where break point to be set</param>
        /// <param name="threadId">Id of the thread (1 or 2)</param>
        /// <returns>bool</returns>
        public bool RemoveBreakPoint(int bytePos, int threadId)
        {
            string cmd = RemoveBreakPointCommand + bytePos.ToString("X") + "," + threadId.ToString();
            string checksum = CalcCheckSum(cmd);
            cmd = "$" + cmd + "#" + checksum;
            return SendCommand(cmd);
        }

        /// <summary>
        /// read memory, this method is called periodically from timer event
        /// and also called explicitly when the user hovers over the variable names in the editor
        /// </summary>
        /// <param name="v"></param>
        /// <param name="addToQ"></param>
        /// <returns></returns>
        public int? ReadMemory(WatchVar v, bool addToQ)
        {
            if (isCmdInProgress) return null;
            if (!QCommands.IsEmpty) return null;
            try
            {
                semaphore.WaitOne();
                isReadMemoryInProgress = true;
                LogReadMemory("Start----------------------------------------Start");
                string cmd = "m" + int.Parse(v.Address).ToString("X") + ",0" + v.Size;
                string checksum = CalcCheckSum(cmd);
                cmd = "$" + cmd + "#" + checksum;

                int count = 0;
                do
                {
                    ClearMessageQ();
                    LogReadMemory("ReadMemory " + cmd);
                    if (addToQ)
                    {
                        QCommands.Enqueue(cmd);
                    }
                    else
                    {
                        SocketComn.Send(cmd);
                    }

                    var mesg = WaitOnQMesg(1);
                    if (mesg != null)
                    {
                        if (mesg == "-\0")
                        {
                            //TODO:: Waiting on firmware programmer
                            //Thread.Sleep(50);                            
                        }
                        else
                        {
                            var response = interpreter.GetMemoryValue(mesg, v.DataType);
                            SocketComn.Send("+");
                            LogReadMemory("ReadMemory [GetMemoryValue] " + cmd);
                            return response;
                        }
                    }
                    count++;
                    Thread.Sleep(WaitTimeInMs);
                    LogReadMemory("ReadMemory [Timeout] " + cmd);
                    SocketComn.Send("+");
                } while (count < NumOfTries);
                return null;
            }
            finally
            {
                LogReadMemory(" End----------------------------------------End ");
                semaphore.Release();
                isReadMemoryInProgress = false;
            }

        }

        /// <summary>
        /// write memory
        /// </summary>
        /// <param name="v"></param>
        public void WriteMemory(WatchVar v)
        {
            var ls = interpreter.GetValToTarget(v.NewValue, v.DataType);
            if (ls != null)
            {
                string cmd = WriteMemoryCommand + int.Parse(v.Address).ToString("X") + ",0" + v.Size + ":" + ls.ToString();
                string checksum = CalcCheckSum(cmd);
                cmd = "$" + cmd + "#" + checksum;
                SendCommand(cmd);
            }
        }


        public bool ContinueAllTasks()
        {
            return SendCommand(ContinueAllCommand, 1);
        }
        public bool ContinueTask0()
        {
            return SendCommand(ContinueTask0Command, 1);
        }
        public bool ContinueTask1()
        {
            return SendCommand(ContinueTask1Command, 1);
        }
        public bool StopAllTasks()
        {
            return SendCommand(StopAllTasksCommand);
        }
        public bool StopTask0()
        {
            return SendCommand(StopTask0Command);
        }
        public bool StopTask1()
        {
            return SendCommand(StopTask1Command);
        }
        public bool StepTask0()
        {
            return SendCommand(StepTask0Command, 1);
        }
        public bool StepTask1()
        {
            return SendCommand(StepTask1Command, 1);
        }
        public void SetMemoryReadFlag(bool able)
        {
            this.readMemoryFlag = able;
        }

        #endregion

        #region private methods
        bool SendCommand(string cmd, int numOfTries = NumOfTries)
        {
            if (WaitIfReadMemoryInProgress())
            {
                return false;
            }

            try
            {
                isCmdInProgress = true;
                semaphore.WaitOne();
                Log(" Start----------------------------------------Start ");
                int count = 0;
                while (count < numOfTries)
                {
                    ClearMessageQ();
                    Log("EnQueue Command " + cmd);
                    QCommands.Enqueue(cmd);
                    var mesg = WaitOnQMesg(1);
                    if (mesg != null)
                    {
                        if (mesg.Contains("$OK"))
                        {
                            Acknowledge("SendCommand " + cmd);
                            return true;
                        }
                        else if (mesg.Contains("+$E02$a7"))
                        {
                            //TODO Check error response & change it to +$E02#a7
                            return false;
                        }
                        else if (mesg == "-\0")
                        {

                        }
                    }
                    count++;
                    Thread.Sleep(WaitTimeInMs);
                    string m = (mesg != null) ? mesg : "";
                    Acknowledge("Acknowledge SendCommand [Timeout] - Try[" + count + "]" + cmd + " Response " + m);
                }
            }
            catch (Exception e)
            {
                logger.LogException(e, "SendCommand " + cmd);
            }
            finally
            {
                Log(" End----------------------------------------End ");
                semaphore.Release();
                isCmdInProgress = false;
            }
            return false;
        }
        bool WaitIfReadMemoryInProgress()
        {
            int waitCount = 0;
            while (isReadMemoryInProgress && waitCount <= 500)
            {
                Thread.Sleep(1);
                waitCount++;
            }
            return (waitCount >= WaitIfReadMemoryInProgressCount);
        }
        void Acknowledge(string msg)
        {
            if (msg != string.Empty)
            {
                Log("Acknowledge for cmd " + msg);
            }
            QCommands.Enqueue("+");
        }

        string WaitOnQMesg(int NumOfMessages, int waitCount = 300)
        {
            int count = 0;
            while (count < waitCount)
            {
                if (!SocketComn.MessageQ.TryDequeue(out string result))
                {
                    Thread.Sleep(1);
                }
                else
                {
                    Console.WriteLine("From que -- >" + result);
                    return result;
                }
                count++;
            }
            return null;
        }


        private void Listener()
        {
            while (IsDebugMode)
            {
                Thread.Sleep(ListenerWaitTimeInMs);
                SocketComn.Recieve();
            }
            Console.WriteLine("end of listener thread");
        }

        int readCnt = 0;
        private void Worker()
        {
            bool isReadMemInProgress = false;
            while (IsDebugMode)
            {
                Thread.Sleep(WorkerWaitTimeInMs);
                if (!QCommands.IsEmpty)
                {
                    if (QCommands.TryDequeue(out string cmd))
                    {
                        SocketComn.Send(cmd);
                    }
                }
                else if (readMemoryFlag && !isReadMemInProgress)
                {
                    readCnt++;
                    if (readCnt >= 2)
                    {
                        readCnt = 0;
                        isReadMemInProgress = true;
                        ReadMemoryLocationInThread();
                        isReadMemInProgress = false;
                    }
                }
            }
            Console.WriteLine("end of worker thread");
        }

        void ReadMemoryLocationInThread()
        {
            foreach (var v in watchVarManager.WatchVars.Values)
            {
                if (!string.IsNullOrEmpty(v.Name))
                {
                    if (!v.Qd && !v.EditMode)
                    {
                        var val = ReadMemory(v, false);
                        v.Qd = true;
                        v.Value = (val != null) ? val.Value.ToString() : "#ERR";
                        WatchVarUpdated(v);
                        CheckAllVarsRead();
                        break;
                    }
                }
            }
        }

        void CheckAllVarsRead()
        {
            bool check = true;
            foreach (var v in watchVarManager.WatchVars.Values)
            {
                if (!string.IsNullOrEmpty(v.Name) && v.Qd == false)
                    check = false;
            }
            if (check)
            {
                foreach (var v in this.watchVarManager.WatchVars.Values)
                {
                    v.Qd = false;
                }
            }
        }
        string CalcCheckSum(string cmd)
        {
            byte total = 0;
            foreach (byte b in System.Text.Encoding.UTF8.GetBytes(cmd.ToCharArray()))
            {
                total += b;
            }
            return total.ToString("X");

        }
        void CheckForStopStatus(string mesg, string haltStr)
        {
            int startIdx = mesg.IndexOf(haltStr);
            if (startIdx >= 0)
            {
                var haltStatusString = mesg.Substring(startIdx, mesg.Length - haltStr.Length);
                HaltStatusEncountered(haltStatusString);
            }
        }
        void HaltStatusEncountered(string obj)
        {
            var byteCodes = interpreter.GetHaltStatus(new List<string>() { obj });
            HaltStatus(byteCodes);
            Acknowledge("Acknowledge for Stop-Status");
        }

        void LogReadMemory(string log)
        {
            logger.LogD(log);
        }

        void Log(string log)
        {
            logger.LogD(log);
        }

        void ClearMessageQ()
        {
            int cnt = 0;
            while (!SocketComn.MessageQ.IsEmpty && cnt <= 100)
            {
                if (SocketComn.MessageQ.TryDequeue(out string result))
                {
                    logger.LogInfo("Clearing MessageQ " + result);
                }
                cnt++;
            }
        }
        bool isQCommandEmpty()
        {
            int cnt = 0;
            while (!QCommands.IsEmpty && cnt <= 100)
            {
                if (QCommands.TryDequeue(out string result))
                {
                    logger.LogInfo("Clearing QCommand " + result);
                }
                cnt++;
            }

            return true;
        }
        #endregion
        
    }
}

