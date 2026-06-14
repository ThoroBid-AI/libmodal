package modal

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"iter"
	"net/http"

	pb "github.com/modal-labs/libmodal/modal-go/proto/modal_proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// VolumeService provides Volume related operations.
type VolumeService interface {
	FromName(ctx context.Context, name string, params *VolumeFromNameParams) (*Volume, error)
	Ephemeral(ctx context.Context, params *VolumeEphemeralParams) (*Volume, error)
	Delete(ctx context.Context, name string, params *VolumeDeleteParams) error
	List(ctx context.Context, params *VolumeListParams) ([]*VolumeListItem, error)
}

type volumeServiceImpl struct{ client *Client }

// Volume represents a Modal Volume that provides persistent storage.
type Volume struct {
	VolumeID        string
	Name            string
	readOnly        bool
	cancelEphemeral context.CancelFunc

	client *Client
}

// VolumeListItem holds summary information about a named volume.
type VolumeListItem struct {
	VolumeID  string
	Name      string
	CreatedAt float64
}

// FileEntry describes a file or directory inside a Volume.
type FileEntry struct {
	Path  string
	Type  FileEntryType
	Mtime uint64
	Size  uint64
}

// FileEntryType mirrors the proto FileEntry.FileType enum.
type FileEntryType int32

const (
	FileEntryTypeUnspecified FileEntryType = 0
	FileEntryTypeFile        FileEntryType = 1
	FileEntryTypeDirectory   FileEntryType = 2
	FileEntryTypeSymlink     FileEntryType = 3
	FileEntryTypeFIFO        FileEntryType = 4
	FileEntryTypeSocket      FileEntryType = 5
)

// VolumeFsVersion selects the Modal Volume filesystem format used when CREATING
// a volume. The zero value lets the server pick its default (currently v1).
type VolumeFsVersion = pb.VolumeFsVersion

const (
	// VolumeVersionUnspecified defers to the server default (currently v1).
	VolumeVersionUnspecified = pb.VolumeFsVersion_VOLUME_FS_VERSION_UNSPECIFIED
	// VolumeVersionV1 is the original Volume format.
	VolumeVersionV1 = pb.VolumeFsVersion_VOLUME_FS_VERSION_V1
	// VolumeVersionV2 is the newer Volume format.
	VolumeVersionV2 = pb.VolumeFsVersion_VOLUME_FS_VERSION_V2
)

// VolumeFromNameParams are options for finding Modal Volumes.
type VolumeFromNameParams struct {
	Environment     string
	CreateIfMissing bool
	// Version selects the filesystem format when CreateIfMissing creates a new
	// volume. Ignored for an already-existing volume (it's returned as-is). The
	// zero value (VolumeVersionUnspecified) lets the server default.
	Version VolumeFsVersion
}

// VolumeEphemeralParams are options for client.Volumes.Ephemeral.
type VolumeEphemeralParams struct {
	Environment string
}

// VolumeDeleteParams are options for client.Volumes.Delete.
type VolumeDeleteParams struct {
	Environment  string
	AllowMissing bool
}

// VolumeListParams are options for client.Volumes.List.
type VolumeListParams struct {
	Environment string
}

// VolumeListDirParams are options for Volume.ListDir.
type VolumeListDirParams struct {
	Recursive bool
}

// VolumeReadFileParams are options for Volume.ReadFile.
type VolumeReadFileParams struct {
	// Start byte offset (default 0).
	Start uint64
	// Len is the number of bytes to read; 0 means read to end.
	Len uint64
}

// VolumeCopyFilesParams are options for Volume.CopyFiles.
type VolumeCopyFilesParams struct {
	Recursive bool
}

// VolumeRemoveFileParams are options for Volume.RemoveFile.
type VolumeRemoveFileParams struct {
	Recursive bool
}

// List returns all named volumes in the given environment.
func (s *volumeServiceImpl) List(ctx context.Context, params *VolumeListParams) ([]*VolumeListItem, error) {
	if params == nil {
		params = &VolumeListParams{}
	}

	resp, err := s.client.cpClient.VolumeList(ctx, pb.VolumeListRequest_builder{
		EnvironmentName: environmentName(params.Environment, s.client.profile),
	}.Build())
	if err != nil {
		return nil, err
	}

	items := make([]*VolumeListItem, 0, len(resp.GetItems()))
	for _, item := range resp.GetItems() {
		name := item.GetLabel()
		if m := item.GetMetadata(); m != nil && m.GetName() != "" {
			name = m.GetName()
		}
		items = append(items, &VolumeListItem{
			VolumeID:  item.GetVolumeId(),
			Name:      name,
			CreatedAt: item.GetCreatedAt(),
		})
	}
	return items, nil
}

// FromName references a Volume by its name.
func (s *volumeServiceImpl) FromName(ctx context.Context, name string, params *VolumeFromNameParams) (*Volume, error) {
	if params == nil {
		params = &VolumeFromNameParams{}
	}

	creationType := pb.ObjectCreationType_OBJECT_CREATION_TYPE_UNSPECIFIED
	if params.CreateIfMissing {
		creationType = pb.ObjectCreationType_OBJECT_CREATION_TYPE_CREATE_IF_MISSING
	}

	resp, err := s.client.cpClient.VolumeGetOrCreate(ctx, pb.VolumeGetOrCreateRequest_builder{
		DeploymentName:     name,
		EnvironmentName:    environmentName(params.Environment, s.client.profile),
		ObjectCreationType: creationType,
		Version:            params.Version,
	}.Build())

	if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound {
		return nil, NotFoundError{fmt.Sprintf("Volume '%s' not found", name)}
	}
	if err != nil {
		return nil, err
	}

	s.client.logger.DebugContext(ctx, "Retrieved Volume", "volume_id", resp.GetVolumeId(), "volume_name", name)
	return &Volume{VolumeID: resp.GetVolumeId(), Name: name, readOnly: false, cancelEphemeral: nil, client: s.client}, nil
}

// ReadOnly configures Volume to mount as read-only.
func (v *Volume) ReadOnly() *Volume {
	return &Volume{
		VolumeID:        v.VolumeID,
		Name:            v.Name,
		readOnly:        true,
		cancelEphemeral: v.cancelEphemeral,
		client:          v.client,
	}
}

// IsReadOnly returns true if the Volume is configured to mount as read-only.
func (v *Volume) IsReadOnly() bool {
	return v.readOnly
}

// Ephemeral creates a nameless, temporary Volume, that persists until CloseEphemeral is called, or the process exits.
func (s *volumeServiceImpl) Ephemeral(ctx context.Context, params *VolumeEphemeralParams) (*Volume, error) {
	if params == nil {
		params = &VolumeEphemeralParams{}
	}

	resp, err := s.client.cpClient.VolumeGetOrCreate(ctx, pb.VolumeGetOrCreateRequest_builder{
		ObjectCreationType: pb.ObjectCreationType_OBJECT_CREATION_TYPE_EPHEMERAL,
		EnvironmentName:    environmentName(params.Environment, s.client.profile),
	}.Build())
	if err != nil {
		return nil, err
	}

	s.client.logger.DebugContext(ctx, "Created ephemeral Volume", "volume_id", resp.GetVolumeId())

	ephemeralCtx, cancel := context.WithCancel(context.Background())
	startEphemeralHeartbeat(ephemeralCtx, func() error {
		_, err := s.client.cpClient.VolumeHeartbeat(ephemeralCtx, pb.VolumeHeartbeatRequest_builder{
			VolumeId: resp.GetVolumeId(),
		}.Build())
		return err
	})

	return &Volume{
		VolumeID:        resp.GetVolumeId(),
		readOnly:        false,
		cancelEphemeral: cancel,
		client:          s.client,
	}, nil
}

// CloseEphemeral deletes an ephemeral Volume, only used with VolumeEphemeral.
func (v *Volume) CloseEphemeral() {
	if v.cancelEphemeral != nil {
		v.cancelEphemeral()
	} else {
		panic(fmt.Sprintf("Volume %s is not ephemeral", v.VolumeID))
	}
}

// Delete deletes a named Volume.
//
// Warning: Deletion is irreversible and will affect any Apps currently using the Volume.
func (s *volumeServiceImpl) Delete(ctx context.Context, name string, params *VolumeDeleteParams) error {
	if params == nil {
		params = &VolumeDeleteParams{}
	}

	volume, err := s.FromName(ctx, name, &VolumeFromNameParams{
		Environment:     params.Environment,
		CreateIfMissing: false,
	})

	if err != nil {
		if _, ok := err.(NotFoundError); ok && params.AllowMissing {
			return nil
		}
		return err
	}

	_, err = s.client.cpClient.VolumeDelete(ctx, pb.VolumeDeleteRequest_builder{
		VolumeId: volume.VolumeID,
	}.Build())

	if err != nil {
		if st, ok := status.FromError(err); ok && st.Code() == codes.NotFound && params.AllowMissing {
			return nil
		}
		return err
	}

	s.client.logger.DebugContext(ctx, "Deleted Volume", "volume_name", name, "volume_id", volume.VolumeID)
	return nil
}

// Rename renames a Volume.
func (v *Volume) Rename(ctx context.Context, newName string) error {
	_, err := v.client.cpClient.VolumeRename(ctx, pb.VolumeRenameRequest_builder{
		VolumeId: v.VolumeID,
		Name:     newName,
	}.Build())
	if err != nil {
		return err
	}
	v.Name = newName
	return nil
}

// Commit persists any changes made to a mounted Volume so they are visible to other containers.
func (v *Volume) Commit(ctx context.Context) error {
	_, err := v.client.cpClient.VolumeCommit(ctx, pb.VolumeCommitRequest_builder{
		VolumeId: v.VolumeID,
	}.Build())
	return err
}

// Reload makes the latest committed state of the Volume available in the running container.
// Reloading will fail if there are open files for the volume.
func (v *Volume) Reload(ctx context.Context) error {
	_, err := v.client.cpClient.VolumeReload(ctx, pb.VolumeReloadRequest_builder{
		VolumeId: v.VolumeID,
	}.Build())
	return err
}

// ListDir lists files and directories under path. Use params.Recursive to recurse.
func (v *Volume) ListDir(ctx context.Context, path string, params *VolumeListDirParams) ([]FileEntry, error) {
	if params == nil {
		params = &VolumeListDirParams{}
	}

	stream, err := v.client.cpClient.VolumeListFiles2(ctx, pb.VolumeListFiles2Request_builder{
		VolumeId:  v.VolumeID,
		Path:      path,
		Recursive: params.Recursive,
	}.Build())
	if err != nil {
		return nil, err
	}

	var entries []FileEntry
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		for _, e := range resp.GetEntries() {
			entries = append(entries, fileEntryFromProto(e))
		}
	}
	return entries, nil
}

// IterDir returns an iterator over files and directories under path.
func (v *Volume) IterDir(ctx context.Context, path string, params *VolumeListDirParams) iter.Seq2[FileEntry, error] {
	if params == nil {
		params = &VolumeListDirParams{}
	}
	return func(yield func(FileEntry, error) bool) {
		stream, err := v.client.cpClient.VolumeListFiles2(ctx, pb.VolumeListFiles2Request_builder{
			VolumeId:  v.VolumeID,
			Path:      path,
			Recursive: params.Recursive,
		}.Build())
		if err != nil {
			yield(FileEntry{}, err)
			return
		}
		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				yield(FileEntry{}, err)
				return
			}
			for _, e := range resp.GetEntries() {
				if !yield(fileEntryFromProto(e), nil) {
					return
				}
			}
		}
	}
}

// ReadFile reads a file from the Volume and returns its contents.
// For large files prefer ReadFileStream to avoid loading everything into memory.
func (v *Volume) ReadFile(ctx context.Context, path string, params *VolumeReadFileParams) ([]byte, error) {
	var buf bytes.Buffer
	for chunk, err := range v.ReadFileStream(ctx, path, params) {
		if err != nil {
			return nil, err
		}
		buf.Write(chunk)
	}
	return buf.Bytes(), nil
}

// ReadFileStream returns an iterator that yields successive byte chunks of a Volume file.
func (v *Volume) ReadFileStream(ctx context.Context, path string, params *VolumeReadFileParams) iter.Seq2[[]byte, error] {
	if params == nil {
		params = &VolumeReadFileParams{}
	}
	return func(yield func([]byte, error) bool) {
		resp, err := v.client.cpClient.VolumeGetFile2(ctx, pb.VolumeGetFile2Request_builder{
			VolumeId: v.VolumeID,
			Path:     path,
			Start:    params.Start,
			Len:      params.Len,
		}.Build())
		if err != nil {
			yield(nil, err)
			return
		}

		for _, url := range resp.GetGetUrls() {
			data, err := fetchURL(ctx, url)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(data, nil) {
				return
			}
		}
	}
}

// RemoveFile removes a file or directory from the Volume.
func (v *Volume) RemoveFile(ctx context.Context, path string, params *VolumeRemoveFileParams) error {
	if params == nil {
		params = &VolumeRemoveFileParams{}
	}
	_, err := v.client.cpClient.VolumeRemoveFile2(ctx, pb.VolumeRemoveFile2Request_builder{
		VolumeId:  v.VolumeID,
		Path:      path,
		Recursive: params.Recursive,
	}.Build())
	return err
}

// CopyFiles copies files within the Volume from srcPaths to dstPath.
// Semantics follow UNIX cp.
func (v *Volume) CopyFiles(ctx context.Context, srcPaths []string, dstPath string, params *VolumeCopyFilesParams) error {
	if params == nil {
		params = &VolumeCopyFilesParams{}
	}
	_, err := v.client.cpClient.VolumeCopyFiles2(ctx, pb.VolumeCopyFiles2Request_builder{
		VolumeId:  v.VolumeID,
		SrcPaths:  srcPaths,
		DstPath:   dstPath,
		Recursive: params.Recursive,
	}.Build())
	return err
}

// fileEntryFromProto converts a proto FileEntry to our local FileEntry type.
func fileEntryFromProto(e *pb.FileEntry) FileEntry {
	return FileEntry{
		Path:  e.GetPath(),
		Type:  FileEntryType(e.GetType()),
		Mtime: e.GetMtime(),
		Size:  e.GetSize(),
	}
}

// fetchURL performs an HTTP GET and returns the response body.
func fetchURL(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected HTTP status %d fetching volume file", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}
