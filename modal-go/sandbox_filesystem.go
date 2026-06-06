package modal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"os"

	pb "github.com/modal-labs/libmodal/modal-go/proto/modal_proto"
)

// SandboxFile represents an open file in the Sandbox filesystem.
// It implements io.Reader, io.Writer, io.Seeker, and io.Closer interfaces.
type SandboxFile struct {
	fileDescriptor string
	taskID         string
	cpClient       pb.ModalClientClient
}

// Read reads up to len(p) bytes from the file into p.
// It returns the number of bytes read and any error encountered.
func (f *SandboxFile) Read(p []byte) (int, error) {
	nBytes := uint32(len(p))
	totalRead, _, err := runFilesystemExec(context.Background(), f.cpClient, pb.ContainerFilesystemExecRequest_builder{
		FileReadRequest: pb.ContainerFileReadRequest_builder{
			FileDescriptor: f.fileDescriptor,
			N:              &nBytes,
		}.Build(),
		TaskId: f.taskID,
	}.Build(), p)
	if err != nil {
		return 0, err
	}
	if totalRead < int(nBytes) {
		return totalRead, io.EOF
	}
	return totalRead, nil
}

// Write writes len(p) bytes from p to the file.
// It returns the number of bytes written and any error encountered.
func (f *SandboxFile) Write(p []byte) (n int, err error) {
	_, _, err = runFilesystemExec(context.Background(), f.cpClient, pb.ContainerFilesystemExecRequest_builder{
		FileWriteRequest: pb.ContainerFileWriteRequest_builder{
			FileDescriptor: f.fileDescriptor,
			Data:           p,
		}.Build(),
		TaskId: f.taskID,
	}.Build(), nil)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}

// Flush flushes any buffered data to the file.
func (f *SandboxFile) Flush() error {
	_, _, err := runFilesystemExec(context.Background(), f.cpClient, pb.ContainerFilesystemExecRequest_builder{
		FileFlushRequest: pb.ContainerFileFlushRequest_builder{
			FileDescriptor: f.fileDescriptor,
		}.Build(),
		TaskId: f.taskID,
	}.Build(), nil)
	if err != nil {
		return err
	}
	return nil
}

// Close closes the file, rendering it unusable for I/O.
func (f *SandboxFile) Close() error {
	_, _, err := runFilesystemExec(context.Background(), f.cpClient, pb.ContainerFilesystemExecRequest_builder{
		FileCloseRequest: pb.ContainerFileCloseRequest_builder{
			FileDescriptor: f.fileDescriptor,
		}.Build(),
		TaskId: f.taskID,
	}.Build(), nil)
	if err != nil {
		return err
	}
	return nil
}

func runFilesystemExec(ctx context.Context, cpClient pb.ModalClientClient, req *pb.ContainerFilesystemExecRequest, p []byte) (int, *pb.ContainerFilesystemExecResponse, error) {
	resp, err := cpClient.ContainerFilesystemExec(ctx, req)
	if err != nil {
		return 0, nil, err
	}
	retries := 10
	totalRead := 0

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		outputIterator, err := cpClient.ContainerFilesystemExecGetOutput(streamCtx, pb.ContainerFilesystemExecGetOutputRequest_builder{
			ExecId:  resp.GetExecId(),
			Timeout: 55,
		}.Build())
		if err != nil {
			if isRetryableGrpc(err) && retries > 0 {
				retries--
				continue
			}
			return 0, nil, err
		}

		for {
			batch, err := outputIterator.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				if isRetryableGrpc(err) && retries > 0 {
					retries--
					break
				}
				return 0, nil, err
			}
			if batch.GetError() != nil {
				return 0, nil, SandboxFilesystemError{batch.GetError().GetErrorMessage()}
			}

			for _, chunk := range batch.GetOutput() {
				copyLen := copy(p[totalRead:], chunk)
				totalRead += copyLen
			}

			if batch.GetEof() {
				return totalRead, resp, nil
			}
		}
	}
}

// fsToolsBinaryPath is the path to the modal sandbox filesystem tools binary.
const fsToolsBinaryPath = "/__modal/.bin/modal-sandbox-fs-tools"

// fsChunkSize is the chunk size for streaming file data to/from the sandbox.
const fsChunkSize = 4 * 1024 * 1024

// SandboxFileType represents the type of a file in the sandbox filesystem.
type SandboxFileType string

const (
	SandboxFileTypeFile      SandboxFileType = "file"
	SandboxFileTypeDirectory SandboxFileType = "directory"
	SandboxFileTypeSymlink   SandboxFileType = "symlink"
)

// SandboxFileInfo contains metadata about a file or directory in the sandbox filesystem.
type SandboxFileInfo struct {
	Name          string
	Path          string
	Type          SandboxFileType
	Size          int64
	Mode          int
	Permissions   string
	Owner         string
	Group         string
	ModifiedTime  float64
	SymlinkTarget string
}

// SandboxFileWatchEventType represents the type of a file watch event.
type SandboxFileWatchEventType string

const (
	SandboxFileWatchEventTypeUnknown SandboxFileWatchEventType = "Unknown"
	SandboxFileWatchEventTypeAccess  SandboxFileWatchEventType = "Access"
	SandboxFileWatchEventTypeCreate  SandboxFileWatchEventType = "Create"
	SandboxFileWatchEventTypeModify  SandboxFileWatchEventType = "Modify"
	SandboxFileWatchEventTypeRemove  SandboxFileWatchEventType = "Remove"
)

// SandboxFileWatchEvent represents a file watch event from the sandbox filesystem.
type SandboxFileWatchEvent struct {
	Paths []string
	Type  SandboxFileWatchEventType
}

// SandboxWatchParams defines optional parameters for Watch.
type SandboxWatchParams struct {
	Recursive bool
	Filter    []SandboxFileWatchEventType // nil means all events
	Timeout   *int                        // seconds, nil means indefinite
}

// SandboxFilesystem provides filesystem operations on a running Sandbox.
type SandboxFilesystem struct {
	sb *Sandbox
}

// fsToolsError is the JSON error payload written to stderr by the fs-tools binary.
type fsToolsError struct {
	ErrorKind string `json:"error_kind"`
	Message   string `json:"message"`
	Detail    string `json:"detail"`
}

// parseFsToolsStderr tries to parse a JSON error from stderr output.
// If the stderr contains a valid JSON error payload, it returns the appropriate error type.
// Otherwise it returns a generic SandboxFilesystemError with the raw message.
func parseFsToolsStderr(stderrData []byte) error {
	msg := string(stderrData)
	if len(stderrData) == 0 {
		return SandboxFilesystemError{Exception: "fs-tools exited with non-zero exit code"}
	}
	var fsErr fsToolsError
	if err := json.Unmarshal(stderrData, &fsErr); err == nil {
		detail := fsErr.Message
		if fsErr.Detail != "" {
			detail = fmt.Sprintf("%s: %s", fsErr.Message, fsErr.Detail)
		}
		switch fsErr.ErrorKind {
		case "NotFound":
			return SandboxFilesystemNotFoundError{Exception: detail}
		case "IsDirectory":
			return SandboxFilesystemIsADirectoryError{Exception: detail}
		case "NotDirectory", "IsFile":
			return SandboxFilesystemNotADirectoryError{Exception: detail}
		case "PermissionDenied":
			return SandboxFilesystemPermissionError{Exception: detail}
		case "PathAlreadyExists":
			return SandboxFilesystemPathAlreadyExistsError{Exception: detail}
		case "DirectoryNotEmpty":
			return SandboxFilesystemDirectoryNotEmptyError{Exception: detail}
		case "NotSupported":
			return InvalidError{Exception: detail}
		default:
			return SandboxFilesystemError{Exception: detail}
		}
	}
	return SandboxFilesystemError{Exception: msg}
}

// execFsTools runs the fs-tools binary with a JSON command string and returns the ContainerProcess.
func (fs *SandboxFilesystem) execFsTools(ctx context.Context, jsonCmd string) (*ContainerProcess, error) {
	return fs.sb.Exec(ctx, []string{fsToolsBinaryPath, jsonCmd}, &SandboxExecParams{})
}

// WriteText writes a text string to a file in the sandbox filesystem.
func (fs *SandboxFilesystem) WriteText(ctx context.Context, data string, remotePath string) error {
	return fs.WriteBytes(ctx, []byte(data), remotePath)
}

// WriteBytes writes raw bytes to a file in the sandbox filesystem.
func (fs *SandboxFilesystem) WriteBytes(ctx context.Context, data []byte, remotePath string) error {
	cmd, _ := json.Marshal(map[string]any{"WriteFile": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return err
	}

	if _, err := proc.Stdin.Write(data); err != nil {
		return err
	}
	if err := proc.Stdin.Close(); err != nil {
		return err
	}

	stderrBytes, _ := io.ReadAll(proc.Stderr)
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return parseFsToolsStderr(stderrBytes)
	}
	return nil
}

// ReadText reads a file from the sandbox filesystem as a string.
func (fs *SandboxFilesystem) ReadText(ctx context.Context, remotePath string) (string, error) {
	data, err := fs.ReadBytes(ctx, remotePath)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// ReadBytes reads a file from the sandbox filesystem as raw bytes.
func (fs *SandboxFilesystem) ReadBytes(ctx context.Context, remotePath string) ([]byte, error) {
	cmd, _ := json.Marshal(map[string]any{"ReadFile": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return nil, err
	}

	// Read stdout and stderr concurrently so we don't deadlock.
	stdoutCh := make(chan []byte, 1)
	stderrCh := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(proc.Stdout)
		stdoutCh <- data
	}()
	go func() {
		data, _ := io.ReadAll(proc.Stderr)
		stderrCh <- data
	}()

	stdoutData := <-stdoutCh
	stderrData := <-stderrCh
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if exitCode != 0 {
		return nil, parseFsToolsStderr(stderrData)
	}
	return stdoutData, nil
}

// sandboxFileInfoJSON is the raw JSON format from the fs-tools binary.
type sandboxFileInfoJSON struct {
	Name          string  `json:"name"`
	Path          string  `json:"path"`
	Type          string  `json:"type"`
	Size          int64   `json:"size"`
	Mode          int     `json:"mode"`
	Permissions   string  `json:"permissions"`
	Owner         string  `json:"owner"`
	Group         string  `json:"group"`
	ModifiedTime  float64 `json:"modified_time"`
	SymlinkTarget string  `json:"symlink_target"`
}

func convertFileInfo(j sandboxFileInfoJSON) SandboxFileInfo {
	return SandboxFileInfo{
		Name:          j.Name,
		Path:          j.Path,
		Type:          SandboxFileType(j.Type),
		Size:          j.Size,
		Mode:          j.Mode,
		Permissions:   j.Permissions,
		Owner:         j.Owner,
		Group:         j.Group,
		ModifiedTime:  j.ModifiedTime,
		SymlinkTarget: j.SymlinkTarget,
	}
}

// ListFiles lists files in a directory in the sandbox filesystem.
func (fs *SandboxFilesystem) ListFiles(ctx context.Context, remotePath string) ([]SandboxFileInfo, error) {
	cmd, _ := json.Marshal(map[string]any{"ListFiles": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return nil, err
	}

	stdoutCh := make(chan []byte, 1)
	stderrCh := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(proc.Stdout)
		stdoutCh <- data
	}()
	go func() {
		data, _ := io.ReadAll(proc.Stderr)
		stderrCh <- data
	}()

	stdoutData := <-stdoutCh
	stderrData := <-stderrCh
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return nil, err
	}
	if exitCode != 0 {
		return nil, parseFsToolsStderr(stderrData)
	}

	var raw []sandboxFileInfoJSON
	if err := json.Unmarshal(stdoutData, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse ListFiles response: %w", err)
	}
	result := make([]SandboxFileInfo, len(raw))
	for i, r := range raw {
		result[i] = convertFileInfo(r)
	}
	return result, nil
}

// Stat returns metadata about a file or directory in the sandbox filesystem.
func (fs *SandboxFilesystem) Stat(ctx context.Context, remotePath string) (SandboxFileInfo, error) {
	cmd, _ := json.Marshal(map[string]any{"Stat": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return SandboxFileInfo{}, err
	}

	stdoutCh := make(chan []byte, 1)
	stderrCh := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(proc.Stdout)
		stdoutCh <- data
	}()
	go func() {
		data, _ := io.ReadAll(proc.Stderr)
		stderrCh <- data
	}()

	stdoutData := <-stdoutCh
	stderrData := <-stderrCh
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return SandboxFileInfo{}, err
	}
	if exitCode != 0 {
		return SandboxFileInfo{}, parseFsToolsStderr(stderrData)
	}

	var raw sandboxFileInfoJSON
	if err := json.Unmarshal(stdoutData, &raw); err != nil {
		return SandboxFileInfo{}, fmt.Errorf("failed to parse Stat response: %w", err)
	}
	return convertFileInfo(raw), nil
}

// MakeDirectory creates a directory in the sandbox filesystem.
func (fs *SandboxFilesystem) MakeDirectory(ctx context.Context, remotePath string, createParents bool) error {
	cmd, _ := json.Marshal(map[string]any{"MakeDirectory": map[string]any{"path": remotePath, "parents": createParents}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return err
	}

	stderrBytes, _ := io.ReadAll(proc.Stderr)
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return parseFsToolsStderr(stderrBytes)
	}
	return nil
}

// Remove removes a file or directory in the sandbox filesystem.
func (fs *SandboxFilesystem) Remove(ctx context.Context, remotePath string, recursive bool) error {
	cmd, _ := json.Marshal(map[string]any{"Remove": map[string]any{"path": remotePath, "recursive": recursive}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return err
	}

	stderrBytes, _ := io.ReadAll(proc.Stderr)
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return parseFsToolsStderr(stderrBytes)
	}
	return nil
}

// CopyFromLocal copies a local file to a path in the sandbox filesystem.
func (fs *SandboxFilesystem) CopyFromLocal(ctx context.Context, localPath string, remotePath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("failed to open local file %s: %w", localPath, err)
	}
	defer f.Close()

	cmd, _ := json.Marshal(map[string]any{"WriteFile": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return err
	}

	stderrCh := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(proc.Stderr)
		stderrCh <- data
	}()

	buf := make([]byte, fsChunkSize)
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if _, writeErr := proc.Stdin.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("failed to write to sandbox stdin: %w", writeErr)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read local file: %w", readErr)
		}
	}

	if err := proc.Stdin.Close(); err != nil {
		return err
	}

	stderrData := <-stderrCh
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return parseFsToolsStderr(stderrData)
	}
	return nil
}

// CopyToLocal copies a file from the sandbox filesystem to a local path.
func (fs *SandboxFilesystem) CopyToLocal(ctx context.Context, remotePath string, localPath string) error {
	cmd, _ := json.Marshal(map[string]any{"ReadFile": map[string]any{"path": remotePath}})
	proc, err := fs.execFsTools(ctx, string(cmd))
	if err != nil {
		return err
	}

	f, err := os.Create(localPath)
	if err != nil {
		return fmt.Errorf("failed to create local file %s: %w", localPath, err)
	}
	defer f.Close()

	stderrCh := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(proc.Stderr)
		stderrCh <- data
	}()

	buf := make([]byte, fsChunkSize)
	for {
		n, readErr := proc.Stdout.Read(buf)
		if n > 0 {
			if _, writeErr := f.Write(buf[:n]); writeErr != nil {
				return fmt.Errorf("failed to write to local file: %w", writeErr)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return fmt.Errorf("failed to read sandbox stdout: %w", readErr)
		}
	}

	stderrData := <-stderrCh
	exitCode, err := proc.Wait(ctx)
	if err != nil {
		return err
	}
	if exitCode != 0 {
		return parseFsToolsStderr(stderrData)
	}
	return nil
}

// watchEventJSON is the raw JSON format for watch events from the fs-tools binary.
type watchEventJSON struct {
	Paths []string `json:"paths"`
	Type  string   `json:"type"`
}

// Watch watches a path in the sandbox filesystem for changes.
// Returns an iterator that yields SandboxFileWatchEvent values.
func (fs *SandboxFilesystem) Watch(ctx context.Context, remotePath string, params *SandboxWatchParams) iter.Seq2[SandboxFileWatchEvent, error] {
	if params == nil {
		params = &SandboxWatchParams{}
	}

	watchCmd := map[string]any{
		"path":      remotePath,
		"recursive": params.Recursive,
		"filter":    nil,
		"timeout_secs": nil,
	}
	if params.Filter != nil {
		filterStrs := make([]string, len(params.Filter))
		for i, f := range params.Filter {
			filterStrs[i] = string(f)
		}
		watchCmd["filter"] = filterStrs
	}
	if params.Timeout != nil {
		watchCmd["timeout_secs"] = *params.Timeout
	}

	cmd, _ := json.Marshal(map[string]any{"Watch": watchCmd})

	return func(yield func(SandboxFileWatchEvent, error) bool) {
		proc, err := fs.execFsTools(ctx, string(cmd))
		if err != nil {
			yield(SandboxFileWatchEvent{}, err)
			return
		}

		stderrCh := make(chan []byte, 1)
		go func() {
			data, _ := io.ReadAll(proc.Stderr)
			stderrCh <- data
		}()

		scanner := bufio.NewScanner(proc.Stdout)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var raw watchEventJSON
			if err := json.Unmarshal(line, &raw); err != nil {
				if !yield(SandboxFileWatchEvent{}, fmt.Errorf("failed to parse watch event: %w", err)) {
					return
				}
				continue
			}

			eventType := SandboxFileWatchEventType(raw.Type)
			// Collapse rename events to Modify
			if raw.Type == "Rename" || raw.Type == "RenameFrom" || raw.Type == "RenameTo" {
				eventType = SandboxFileWatchEventTypeModify
			}

			event := SandboxFileWatchEvent{
				Paths: raw.Paths,
				Type:  eventType,
			}
			if !yield(event, nil) {
				return
			}
		}

		if err := scanner.Err(); err != nil {
			yield(SandboxFileWatchEvent{}, fmt.Errorf("error reading watch output: %w", err))
		}

		stderrData := <-stderrCh
		exitCode, waitErr := proc.Wait(ctx)
		if waitErr != nil {
			yield(SandboxFileWatchEvent{}, waitErr)
			return
		}
		if exitCode != 0 {
			yield(SandboxFileWatchEvent{}, parseFsToolsStderr(stderrData))
		}
	}
}
