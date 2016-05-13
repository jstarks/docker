package schema2

import (
	"github.com/docker/distribution"
	"github.com/docker/distribution/context"
	"github.com/docker/distribution/digest"
)

// builder is a type for constructing manifests.
type builder struct {
	// bs is a BlobService used to publish the configuration blob.
	bs distribution.BlobService

	// configJSON references
	configJSON []byte

	// layers is a list of layer descriptors that gets built by successive
	// calls to AppendReference.
	layers []LayerDescriptor
}

// NewManifestBuilder is used to build new manifests for the current schema
// version. It takes a BlobService so it can publish the configuration blob
// as part of the Build process.
func NewManifestBuilder(bs distribution.BlobService, configJSON []byte) distribution.ManifestBuilder {
	mb := &builder{
		bs:         bs,
		configJSON: make([]byte, len(configJSON)),
	}
	copy(mb.configJSON, configJSON)

	return mb
}

// Build produces a final manifest from the given references.
func (mb *builder) Build(ctx context.Context) (distribution.Manifest, error) {
	m := Manifest{
		Versioned: SchemaVersion,
		Layers:    make([]LayerDescriptor, len(mb.layers)),
	}
	copy(m.Layers, mb.layers)

	configDigest := digest.FromBytes(mb.configJSON)

	var err error
	m.Config, err = mb.bs.Stat(ctx, configDigest)
	switch err {
	case nil:
		return FromStruct(m)
	case distribution.ErrBlobUnknown:
		// nop
	default:
		return nil, err
	}

	// Add config to the blob store
	m.Config, err = mb.bs.Put(ctx, MediaTypeConfig, mb.configJSON)
	// Override MediaType, since Put always replaces the specified media
	// type with application/octet-stream in the descriptor it returns.
	m.Config.MediaType = MediaTypeConfig
	if err != nil {
		return nil, err
	}

	return FromStruct(m)
}

// LayerDescribable is an interface used to provide a full LayerDescriptor
// to AppendReference().
type LayerDescribable interface {
	LayerDescriptor() LayerDescriptor
}

// AppendReference adds a reference to the current ManifestBuilder.
func (mb *builder) AppendReference(d distribution.Describable) error {
	if ld, ok := d.(LayerDescribable); ok {
		mb.layers = append(mb.layers, ld.LayerDescriptor())
	} else {
		mb.layers = append(mb.layers, LayerDescriptor{Descriptor: d.Descriptor()})
	}
	return nil
}

// References returns the current references added to this builder.
func (mb *builder) References() []distribution.Descriptor {
	ds := make([]distribution.Descriptor, len(mb.layers))
	for i := range mb.layers {
		ds[i] = mb.layers[i].Descriptor
	}
	return ds
}
