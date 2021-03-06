use strict;
use warnings;
use Module::Build;

my $builder = Module::Build->new(
    module_name       => 'ElasticSearch',
    license           => 'perl',
    dist_author       => q{Clinton Gormley <drtech@cpan.org>},
    dist_version_from => 'lib/ElasticSearch.pm',
    build_requires    => {
        'Test::Most' => 0,
        'POSIX'      => 0,
        'IO::Socket' => 0,
        'File::Temp' => '0.22',
    },
    add_to_cleanup     => ['ElasticSearch-*'],
    create_makefile_pl => 'traditional',
    requires           => {
        'LWP::UserAgent' => 0,
        'LWP::ConnCache' => 0,
        'HTTP::Request'  => 0,
        'JSON'           => 0,
        'Data::Dumper'   => 0,
        'Encode'         => 0,
        'HTTP::Lite'     => 0,
        'parent'         => 0,
    },
    recommends => {
        'Bit::Vector'     => 0,
        'Class::Accessor' => 0,
        'JSON::XS'        => 0,
    }
);

$builder->create_build_script();
