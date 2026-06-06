package test

import (
	"context"
	"testing"

	"github.com/modal-labs/libmodal/modal-go"
	"github.com/modal-labs/libmodal/modal-go/internal/grpcmock"
	pb "github.com/modal-labs/libmodal/modal-go/proto/modal_proto"
	"github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestVolumeFromName(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tc := newTestClient(t)

	volume, err := tc.Volumes.FromName(ctx, "libmodal-test-volume", &modal.VolumeFromNameParams{
		CreateIfMissing: true,
	})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(volume).ShouldNot(gomega.BeNil())
	g.Expect(volume.VolumeID).Should(gomega.HavePrefix("vo-"))
	g.Expect(volume.Name).To(gomega.Equal("libmodal-test-volume"))

	_, err = tc.Volumes.FromName(ctx, "missing-volume", nil)
	g.Expect(err).Should(gomega.MatchError(gomega.ContainSubstring("Volume 'missing-volume' not found")))
}

func TestVolumeReadOnly(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()
	tc := newTestClient(t)

	volume, err := tc.Volumes.FromName(ctx, "libmodal-test-volume", &modal.VolumeFromNameParams{
		CreateIfMissing: true,
	})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(volume.IsReadOnly()).To(gomega.BeFalse())

	readOnlyVolume := volume.ReadOnly()
	g.Expect(readOnlyVolume.IsReadOnly()).To(gomega.BeTrue())
	g.Expect(readOnlyVolume.VolumeID).To(gomega.Equal(volume.VolumeID))
	g.Expect(readOnlyVolume.Name).To(gomega.Equal(volume.Name))

	g.Expect(volume.IsReadOnly()).To(gomega.BeFalse())
}

func TestVolumeEphemeral(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	tc := newTestClient(t)

	volume, err := tc.Volumes.Ephemeral(context.Background(), nil)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	defer volume.CloseEphemeral()
	g.Expect(volume.Name).To(gomega.BeEmpty())
	g.Expect(volume.VolumeID).Should(gomega.HavePrefix("vo-"))
	g.Expect(volume.IsReadOnly()).To(gomega.BeFalse())
	g.Expect(volume.ReadOnly().IsReadOnly()).To(gomega.BeTrue())
}

func TestVolumeDeleteSuccess(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(
		mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{
				VolumeId: "vo-test-123",
			}.Build(), nil
		},
	)

	grpcmock.HandleUnary(
		mock, "/VolumeDelete",
		func(req *pb.VolumeDeleteRequest) (*emptypb.Empty, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-test-123"))
			return &emptypb.Empty{}, nil
		},
	)

	err := mock.Volumes.Delete(ctx, "test-volume", nil)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeDeleteWithAllowMissing(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(
		mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return nil, modal.NotFoundError{Exception: "Volume 'missing' not found"}
		},
	)

	err := mock.Volumes.Delete(ctx, "missing", &modal.VolumeDeleteParams{
		AllowMissing: true,
	})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeDeleteWithAllowMissingDeleteRPCNotFound(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-test-123"}.Build(), nil
		},
	)

	grpcmock.HandleUnary(mock, "/VolumeDelete",
		func(req *pb.VolumeDeleteRequest) (*emptypb.Empty, error) {
			return nil, status.Errorf(codes.NotFound, "Volume not found")
		},
	)

	err := mock.Volumes.Delete(ctx, "test-volume", &modal.VolumeDeleteParams{AllowMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeDeleteWithAllowMissingFalseThrows(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(
		mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return nil, modal.NotFoundError{Exception: "Volume 'missing' not found"}
		},
	)

	err := mock.Volumes.Delete(ctx, "missing", &modal.VolumeDeleteParams{
		AllowMissing: false,
	})
	g.Expect(err).Should(gomega.HaveOccurred())
	var notFoundErr modal.NotFoundError
	g.Expect(err).Should(gomega.BeAssignableToTypeOf(notFoundErr))

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeList(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(
		mock, "/VolumeList",
		func(req *pb.VolumeListRequest) (*pb.VolumeListResponse, error) {
			return pb.VolumeListResponse_builder{
				Items: []*pb.VolumeListItem{
					pb.VolumeListItem_builder{
						VolumeId: "vo-aaa",
						Metadata: pb.VolumeMetadata_builder{Name: "vol-a"}.Build(),
					}.Build(),
					pb.VolumeListItem_builder{
						VolumeId: "vo-bbb",
						Metadata: pb.VolumeMetadata_builder{Name: "vol-b"}.Build(),
					}.Build(),
				},
			}.Build(), nil
		},
	)

	items, err := mock.Volumes.List(ctx, nil)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(items).To(gomega.HaveLen(2))
	g.Expect(items[0].VolumeID).To(gomega.Equal("vo-aaa"))
	g.Expect(items[0].Name).To(gomega.Equal("vol-a"))
	g.Expect(items[1].VolumeID).To(gomega.Equal("vo-bbb"))
	g.Expect(items[1].Name).To(gomega.Equal("vol-b"))

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeCommit(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-test-commit"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeCommit",
		func(req *pb.VolumeCommitRequest) (*pb.VolumeCommitResponse, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-test-commit"))
			return pb.VolumeCommitResponse_builder{}.Build(), nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "test-vol", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.Commit(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeReload(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-test-reload"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeReload",
		func(req *pb.VolumeReloadRequest) (*emptypb.Empty, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-test-reload"))
			return &emptypb.Empty{}, nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "test-vol", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.Reload(ctx)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeRename(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-rename-me"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeRename",
		func(req *pb.VolumeRenameRequest) (*emptypb.Empty, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-rename-me"))
			g.Expect(req.GetName()).To(gomega.Equal("new-name"))
			return &emptypb.Empty{}, nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "old-name", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.Rename(ctx, "new-name")
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	g.Expect(vol.Name).To(gomega.Equal("new-name"))

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeRemoveFile(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-rm-file"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeRemoveFile2",
		func(req *pb.VolumeRemoveFile2Request) (*emptypb.Empty, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-rm-file"))
			g.Expect(req.GetPath()).To(gomega.Equal("/data/old.txt"))
			g.Expect(req.GetRecursive()).To(gomega.BeFalse())
			return &emptypb.Empty{}, nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "test-vol", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.RemoveFile(ctx, "/data/old.txt", nil)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeRemoveFileRecursive(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-rm-dir"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeRemoveFile2",
		func(req *pb.VolumeRemoveFile2Request) (*emptypb.Empty, error) {
			g.Expect(req.GetRecursive()).To(gomega.BeTrue())
			g.Expect(req.GetPath()).To(gomega.Equal("/data/subdir"))
			return &emptypb.Empty{}, nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "test-vol", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.RemoveFile(ctx, "/data/subdir", &modal.VolumeRemoveFileParams{Recursive: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}

func TestVolumeCopyFiles(t *testing.T) {
	t.Parallel()
	g := gomega.NewWithT(t)
	ctx := context.Background()

	mock := newGRPCMockClient(t)

	grpcmock.HandleUnary(mock, "/VolumeGetOrCreate",
		func(req *pb.VolumeGetOrCreateRequest) (*pb.VolumeGetOrCreateResponse, error) {
			return pb.VolumeGetOrCreateResponse_builder{VolumeId: "vo-copy"}.Build(), nil
		},
	)
	grpcmock.HandleUnary(mock, "/VolumeCopyFiles2",
		func(req *pb.VolumeCopyFiles2Request) (*emptypb.Empty, error) {
			g.Expect(req.GetVolumeId()).To(gomega.Equal("vo-copy"))
			g.Expect(req.GetSrcPaths()).To(gomega.ConsistOf("/src/a.txt", "/src/b.txt"))
			g.Expect(req.GetDstPath()).To(gomega.Equal("/dst/"))
			g.Expect(req.GetRecursive()).To(gomega.BeFalse())
			return &emptypb.Empty{}, nil
		},
	)

	vol, err := mock.Volumes.FromName(ctx, "test-vol", &modal.VolumeFromNameParams{CreateIfMissing: true})
	g.Expect(err).ShouldNot(gomega.HaveOccurred())
	err = vol.CopyFiles(ctx, []string{"/src/a.txt", "/src/b.txt"}, "/dst/", nil)
	g.Expect(err).ShouldNot(gomega.HaveOccurred())

	g.Expect(mock.AssertExhausted()).ShouldNot(gomega.HaveOccurred())
}
